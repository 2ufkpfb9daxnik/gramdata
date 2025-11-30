"""
IN_DIR/*.jsonl の text を sudachipy (sudachidict_full) で形態素解析し、
1..7-gram を厳密に集計（SQLite）して OUT_DIR に
{n}hplt{index:04d}.txt を出力するスクリプト。

出力は "<形態素列><TAB><出現回数>" を頻度順で並べ、
MIN_COUNT 未満は出力しません。
ファイルサイズは SIZE_MB を超えたら新しいインデックスに分割します。
N_FILES_PER_PUSH 個の新規ファイルごとに git add/commit/push します。
"""
import os
import sys
import json
import re
import sqlite3
import subprocess
import time
import random
from pathlib import Path
from collections import Counter

# --- 設定（必要に応じ修正）---
SUDACHI_FULL_RES = r'D:\gramdata\.venv\Lib\site-packages\sudachidict_full\resources'
IN_DIR = Path("data")                       # 入力 JSONL ディレクトリ（相対/絶対）
PATTERN = "10_*.jsonl"
OUT_DIR = Path("word")                      # 出力ディレクトリ（例: D:\gramdata\hplt\word）
MIN_COUNT = 10                              # 出力に含める最低頻度
NGRAM_MAX = 7
SIZE_MB = 50                                # 分割閾値（MB）
DB_PATH = OUT_DIR / "ngrams.sqlite"         # 内部 DB ファイル
DB_BATCH_SIZE = 5000                        # DB にバッチで書き込む鍵数の目安
N_FILES_PER_PUSH = 5                        # この数だけ新ファイルができたら git push
GIT_COMMIT_MESSAGE_PREFIX = "add ngram files"
# -------------------------------

# Sudachi の辞書指定は import 前にセット
os.environ["SUDACHIPY_DICT"] = SUDACHI_FULL_RES

try:
    from sudachipy import dictionary, tokenizer
    from sudachipy.errors import SudachiError
except Exception as e:
    print("SudachiPy import error; SUDACHI_FULL_RES を確認してください:", e, file=sys.stderr)
    raise SystemExit(1)

# 日本語トークン判定（句読点・長音符・全角記号・ダッシュ含む）
_JP_RE = re.compile(
    r'['
    r'\u3040-\u309F'
    r'\u30A0-\u30FF'
    r'\u4E00-\u9FFF'
    r'\u3000-\u303F'
    r'\uFF00-\uFFEF'
    r'\u2010-\u2015'
    r']'
)

def is_japanese_token(s: str) -> bool:
    return bool(_JP_RE.search(s))

def jsonl_iter_texts(path: Path):
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

# 辞書/Tokenizer の初期化（堅牢に試行）
def _create_tokenizer(res_path: Path):
    try:
        return dictionary.Dictionary().create()
    except Exception:
        pass
    sudachi_json = res_path / "sudachi.json"
    system_dic = res_path / "system.dic"
    if sudachi_json.exists() and system_dic.exists():
        # 一時設定ファイルで systemDict を絶対パス指定して試す
        import tempfile, json, importlib
        try:
            tpl = json.loads(sudachi_json.read_text(encoding="utf-8"))
        except Exception:
            tpl = {}
        tpl["systemDict"] = str(system_dic.resolve())
        if "characterDefinitionFile" not in tpl:
            tpl["characterDefinitionFile"] = "char.def"
        tf = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
        try:
            json.dump(tpl, tf, ensure_ascii=False, indent=2)
            tf.flush()
            tf.close()
            try:
                return dictionary.Dictionary(str(tf.name)).create()
            except Exception:
                importlib.reload(dictionary)
                return dictionary.Dictionary(str(tf.name)).create()
        finally:
            try:
                os.unlink(tf.name)
            except Exception:
                pass
    raise RuntimeError("Sudachi dictionary init failed; check sudachidict_full resources")

# DB 初期化
def init_db(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode = WAL;")
    cur.execute("PRAGMA synchronous = NORMAL;")
    cur.execute("PRAGMA temp_store = MEMORY;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS counts (
            n INTEGER NOT NULL,
            gram TEXT NOT NULL,
            cnt INTEGER NOT NULL,
            PRIMARY KEY(n, gram)
        )
    """)
    conn.commit()
    return conn

# バッチを DB に適用（batch: dict[(n,gram)] -> count）
def flush_batch_to_db(conn, batch):
    if not batch:
        return
    cur = conn.cursor()
    items = [(n, gram, cnt) for (n, gram), cnt in batch.items()]
    # UPSERT: excluded.cnt を使して加算
    cur.executemany(
        "INSERT INTO counts(n, gram, cnt) VALUES(?, ?, ?) "
        "ON CONFLICT(n, gram) DO UPDATE SET cnt = cnt + excluded.cnt",
        items
    )
    conn.commit()
    batch.clear()

# 出力ファイルを分割して書き出す（SIZE_MB を上限にインデックス増やす）
def export_counts_to_files(conn, out_dir: Path, min_count: int, n_max: int, size_mb: int, git_push_batch: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    cur = conn.cursor()
    created_files = []
    target_bytes = size_mb * 1024 * 1024
    for n in range(1, n_max + 1):
        q = cur.execute(
            "SELECT gram, cnt FROM counts WHERE n=? AND cnt>=? ORDER BY cnt DESC, gram ASC",
            (n, min_count)
        )
        idx = 0
        f = None
        bytes_written = 0
        for gram, cnt in q:
            line = f"{gram}\t{cnt}\n"
            b = len(line.encode("utf-8"))
            if f is None:
                path = out_dir / f"{n}hplt{idx:04d}.txt"
                f = path.open("w", encoding="utf-8")
                bytes_written = 0
            # file rotate if adding this line exceeds target and file not empty
            if bytes_written + b > target_bytes and bytes_written > 0:
                f.close()
                created_files.append(path)
                idx += 1
                path = out_dir / f"{n}hplt{idx:04d}.txt"
                f = path.open("w", encoding="utf-8")
                bytes_written = 0
            f.write(line)
            bytes_written += b
        if f is not None:
            f.close()
            created_files.append(out_dir / f"{n}hplt{idx:04d}.txt")
    # Git push in batches of git_push_batch files
    if created_files and git_push_batch > 0:
        repo_root = _get_git_root(out_dir) or _get_git_root(Path.cwd())
        if repo_root is not None:
            for i in range(0, len(created_files), git_push_batch):
                batch = created_files[i:i+git_push_batch]
                _git_add_commit_push(batch, repo_root)
    return created_files

def _get_git_root(start_path: Path):
    try:
        p = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                           cwd=str(start_path), check=True, capture_output=True, text=True)
        return Path(p.stdout.strip())
    except Exception:
        return None

def _git_add_commit_push(files, repo_root: Path):
    rels = []
    for f in files:
        try:
            rel = Path(f).resolve().relative_to(repo_root.resolve())
            rels.append(str(rel).replace("\\", "/"))
        except Exception:
            rels.append(str(Path(f).resolve()))
    msg = f"{GIT_COMMIT_MESSAGE_PREFIX}: " + ", ".join(rels)
    attempt = 0
    while attempt < 6:
        attempt += 1
        try:
            subprocess.run(["git", "add", "--"] + rels, cwd=str(repo_root), check=True)
            # commit only if there is something staged
            diff = subprocess.run(["git", "diff", "--cached", "--name-only"], cwd=str(repo_root),
                                  check=True, capture_output=True, text=True)
            if not diff.stdout.strip():
                return True
            subprocess.run(["git", "commit", "-m", msg], cwd=str(repo_root), check=True)
            subprocess.run(["git", "push"], cwd=str(repo_root), check=True)
            return True
        except subprocess.CalledProcessError as e:
            time.sleep(min((2 ** attempt) + random.random(), 60))
    return False

def main():
    res_path = Path(SUDACHI_FULL_RES)
    if not res_path.is_dir() or not (res_path / "system.dic").exists():
        print("辞書 resources が見つからないか system.dic がありません:", res_path, file=sys.stderr)
        raise SystemExit(1)
    if not IN_DIR.exists():
        print("入力ディレクトリが存在しません:", IN_DIR, file=sys.stderr)
        raise SystemExit(1)
    files = sorted(IN_DIR.glob(PATTERN))
    if not files:
        print("処理対象ファイルが見つかりません。", file=sys.stderr)
        return

    try:
        tok = _create_tokenizer(res_path)
    except Exception as e:
        print("辞書初期化エラー:", e, file=sys.stderr)
        raise SystemExit(1)

    split_mode = tokenizer.Tokenizer.SplitMode.B
    conn = init_db(DB_PATH)
    batch = {}  # dict[(n,gram)] -> count

    def add_to_batch(n, gram):
        key = (n, gram)
        batch[key] = batch.get(key, 0) + 1
        if len(batch) >= DB_BATCH_SIZE:
            flush_batch_to_db(conn, batch)

    for src in files:
        print("processing", src.name)
        for text in jsonl_iter_texts(src):
            try:
                ms = tok.tokenize(text, split_mode)
            except Exception:
                ms = []
            surfaces = [m.surface() for m in ms if is_japanese_token(m.surface())]
            if not surfaces:
                continue
            L = len(surfaces)
            for n in range(1, min(NGRAM_MAX, L) + 1):
                for i in range(0, L - n + 1):
                    gram = " ".join(surfaces[i:i+n])
                    add_to_batch(n, gram)

    # flush remaining
    flush_batch_to_db(conn, batch)
    # export to files (split by SIZE_MB) and push to git in batches
    created = export_counts_to_files(conn, OUT_DIR, MIN_COUNT, NGRAM_MAX, SIZE_MB, N_FILES_PER_PUSH)
    conn.close()
    print("完了。出力先:", OUT_DIR)
    print("生成ファイル数:", len(created))

if __name__ == "__main__":
    main()