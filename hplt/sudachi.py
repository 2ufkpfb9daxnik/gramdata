"""
Sudachi を使って hplt データの JSONL (text フィールド) から日本語トークンを抽出し、
1-7 gram をカウントして {n}hplt{index:04d}.txt 形式で出力するスクリプト。
- 設定は下の定数を直接編集してください（引数は使いません）。
- 出力先: OUT_DIR（デフォルト: d:\gramdata\hplt\data 相対）
- ファイルは目標サイズ（SIZE_MB）ごとにローテーション
- 生成したファイルを GIT_BATCH 個ごとに自動で git add/commit/push（ENABLE_GIT=True の場合）
"""
from pathlib import Path
import json
import sys
import re
import time
import random
import subprocess
from collections import Counter

# Sudachi import
try:
    from sudachipy import dictionary, tokenizer as sud_tokenizer
except Exception:
    print("SudachiPy が必要です: pip install sudachipy sudachidict_core などの辞書を導入してください", file=sys.stderr)
    raise SystemExit(1)

# --- 設定（ここを直接変更してください） ---
IN_DIR = Path(".")                # 入力ディレクトリ（実行場所に合わせる）
PATTERN = "10_*.jsonl"            # JSONL ファイルパターン
OUT_DIR = Path("data")            # 出力先（例: gramdata/hplt/data）
SIZE_MB = 50                      # 目標ファイルサイズ（MB）
ENABLE_GIT = True                 # True で自動コミット/プッシュ
GIT_BATCH = 5                     # 何ファイルごとにプッシュするか
NGRAM_MAX = 7                     # n-gram の最大 n
GIT_RETRIES = 6                   # push リトライ回数
SPLIT_MODE = sud_tokenizer.Tokenizer.SplitMode.B  # B は MeCab と近い粒度のことが多い
# ------------------------------------------------

_pending_files = []
_repo_root = None

# 日本語判定（ひらがな/カタカナ/漢字 と CJK 句読点・全角記号を含むトークンを日本語とみなす）
_JP_RE = re.compile(
    r'['
    r'\u3040-\u309F'  # ひらがな
    r'\u30A0-\u30FF'  # カタカナ
    r'\u4E00-\u9FFF'  # 漢字（CJK 統合漢字）
    r'\u3000-\u303F'  # CJK Symbols and Punctuation（。、・「」等）
    r'\uFF00-\uFFEF'  # 半角/全角記号
    r']'
)

def is_japanese_token(s: str) -> bool:
    return bool(_JP_RE.search(s))

def get_git_root(start_path: Path):
    try:
        p = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                           cwd=str(start_path), check=True, capture_output=True, text=True)
        return Path(p.stdout.strip())
    except subprocess.CalledProcessError:
        return None

def commit_and_push(files, repo_root: Path, retries=GIT_RETRIES):
    if not files:
        return True
    rel_paths = []
    for f in files:
        try:
            rel = Path(f).resolve().relative_to(repo_root.resolve())
            rel_paths.append(str(rel).replace("\\", "/"))
        except Exception:
            rel_paths.append(str(Path(f).resolve()))
    msg_paths = ", ".join(f"({p})" for p in rel_paths)
    msg = f"{msg_paths} を追加"
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            subprocess.run(["git", "add", "--"] + rel_paths, cwd=str(repo_root), check=True)
            diff = subprocess.run(["git", "diff", "--cached", "--name-only"],
                                  cwd=str(repo_root), check=True, capture_output=True, text=True)
            if not diff.stdout.strip():
                print("コミットする差分がありません、スキップします。")
                return True
            subprocess.run(["git", "commit", "-m", msg], cwd=str(repo_root), check=True)
            subprocess.run(["git", "push"], cwd=str(repo_root), check=True)
            print(f"git push 成功: {len(files)} 件 -> {msg}")
            return True
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "").strip()
            stdout = (e.stdout or "").strip()
            print(f"git 操作失敗 (試行 {attempt}/{retries}): stdout={stdout} stderr={stderr}", file=sys.stderr)
            backoff = min((2 ** attempt) + random.uniform(0, 3), 300)
            print(f"{backoff:.1f}s 後に再試行します...", file=sys.stderr)
            time.sleep(backoff)
    print("git push が最大試行回数で失敗しました。手動で確認してください。", file=sys.stderr)
    return False

def _maybe_flush_pending():
    global _pending_files, _repo_root
    if not ENABLE_GIT or _repo_root is None:
        return
    if len(_pending_files) >= GIT_BATCH:
        to_commit = _pending_files[:]
        _pending_files = []
        commit_and_push(to_commit, _repo_root)

def jsonl_iter_texts(path: Path):
    """
    JSONL を一行ずつ読み、"text" フィールドの文字列を yield する。
    フォールバックで簡易抽出も試みる。
    """
    txt_re = re.compile(r'"text"\s*:\s*"')
    for raw in path.open("r", encoding="utf-8", errors="replace"):
        line = raw.rstrip("\n")
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                t = obj.get("text") or obj.get("body") or obj.get("content")
                if t:
                    yield t
                    continue
        except Exception:
            if txt_re.search(line):
                # 簡易抽出（完全ではないが fallback）
                m2 = re.search(r'"text"\s*:\s*"((?:\\.|[^"\\])*)"', line)
                if m2:
                    rawtxt = m2.group(1)
                    try:
                        t = bytes(rawtxt, "utf-8").decode("unicode_escape")
                        yield t
                        continue
                    except Exception:
                        pass
            continue

def tokenize_sudachi(text: str, tok):
    """
    Sudachi でトークン化し、日本語と判定できるトークンだけ返す（表層形）
    """
    try:
        ms = tok.tokenize(text, SPLIT_MODE)
    except Exception:
        # 失敗したら空
        return []
    surfaces = []
    for m in ms:
        s = m.surface()
        if is_japanese_token(s):
            surfaces.append(s)
    return surfaces

def write_counter_to_file(counter: Counter, out_dir: Path, n: int, index: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{n}hplt{index:04d}.txt"
    with out_path.open("w", encoding="utf-8") as f:
        for gram, cnt in counter.most_common():
            f.write(f"{gram}\t{cnt}\n")
    return out_path

def estimate_counter_size_bytes(counter: Counter):
    total = 0
    for k, v in counter.items():
        total += len(k.encode("utf-8")) + 1 + len(str(v)) + 1
    return total

def process_files():
    global _repo_root, _pending_files
    in_dir = IN_DIR.resolve()
    out_dir = OUT_DIR.resolve()
    target_bytes = SIZE_MB * 1024 * 1024

    if not in_dir.exists():
        print("入力ディレクトリが存在しません。", file=sys.stderr)
        return

    if ENABLE_GIT:
        _repo_root = get_git_root(out_dir) or get_git_root(in_dir) or get_git_root(Path.cwd())
        if _repo_root is None:
            print("警告: git リポジトリが見つかりません。自動コミットは無効になります。", file=sys.stderr)

    # Sudachi tokenizer 作成
    sud = dictionary.Dictionary().create()

    counters = {n: Counter() for n in range(1, NGRAM_MAX+1)}
    indexes = {n: 0 for n in range(1, NGRAM_MAX+1)}
    sizes = {n: 0 for n in range(1, NGRAM_MAX+1)}

    files = sorted(in_dir.glob(PATTERN))
    if not files:
        print("処理対象ファイルが見つかりません。", file=sys.stderr)
        return

    for src in files:
        print(f"処理中: {src.name}")
        for text in jsonl_iter_texts(src):
            tokens = tokenize_sudachi(text, sud)
            if not tokens:
                continue
            L = len(tokens)
            for n in range(1, min(NGRAM_MAX, L)+1):
                c = counters[n]
                for i in range(0, L - n + 1):
                    gram = " ".join(tokens[i:i+n])
                    c[gram] += 1
                sizes[n] = estimate_counter_size_bytes(c)
            for n in range(1, NGRAM_MAX+1):
                if sizes[n] >= target_bytes and counters[n]:
                    p = write_counter_to_file(counters[n], out_dir, n, indexes[n])
                    indexes[n] += 1
                    _pending_files.append(str(p))
                    counters[n].clear()
                    sizes[n] = 0
                    _maybe_flush_pending()

    # final flush
    for n in range(1, NGRAM_MAX+1):
        if counters[n]:
            p = write_counter_to_file(counters[n], out_dir, n, indexes[n])
            indexes[n] += 1
            _pending_files.append(str(p))
            counters[n].clear()
            _maybe_flush_pending()

    # 最後の残りを push
    if ENABLE_GIT and _repo_root is not None and _pending_files:
        to_commit = _pending_files[:]
        _pending_files = []
        commit_and_push(to_commit, _repo_root)

    print("完了。")

if __name__ == "__main__":
    process_files()