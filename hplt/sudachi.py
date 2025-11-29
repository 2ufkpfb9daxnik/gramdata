#!/usr/bin/env python3
"""
並列チャンク+マージ方式で 1..NGRAM_MAX を厳密集計する完全版。

- ワーカーは各自 sudachidict_full を参照してトークン化し、
  N ごとの部分カウントを OUT_DIR/parts/{n}/ に sorted part ファイルとして書き出します。
- パートファイルはワーカー内で SIZE_MB を目安に flush されます（推定）。
- メインは全パートを k-way merge して OUT_DIR/{n}hplt0000.txt を出力（freq >= MIN_COUNT）。
- パートファイルが GIT_BATCH 個溜まるごとに自動で git add/commit/push を試みます。
- Windows (multiprocessing) に対応。仮想環境を有効にして実行してください。

実行:
  (.venv) PS D:\gramdata\hplt> python d:\gramdata\hplt\sudachi.py
"""
import os
import json
import re
import sys
import tempfile
import importlib
import subprocess
import time
import random
import threading
from pathlib import Path
from collections import Counter
import multiprocessing as mp
import uuid
import heapq

# --- 設定（必要なら編集） ---
SUDACHI_FULL_RES = r'D:\gramdata\.venv\Lib\site-packages\sudachidict_full\resources'
IN_DIR = Path("data")
PATTERN = "10_*.jsonl"
OUT_DIR = Path("word")
MIN_COUNT = 10          # 出力に含める最小出現回数
NGRAM_MAX = 7
SIZE_MB = 50            # ワーカーが一時 flush する目安（MB）
WORKERS = max(1, mp.cpu_count() - 1)
# Git 関連
GIT_BATCH = 5           # 何個のパートで push するか
GIT_RETRIES = 6
# ------------------------------

# 日本語トークン判定（ひらがな/カタカナ/漢字 と CJK 句読点・全角記号・ダッシュ）
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

def _create_sudachi_tokenizer_local(res_path_str: str):
    """
    ワーカー内で呼ぶ。SUDACHIPY_DICT をセットして sudachipy を import し、
    Tokenizer インスタンスと tokenizer モジュールを返す。
    フォールバックで一時 sudachi.json を作って absolute system.dic を指す。
    """
    os.environ['SUDACHIPY_DICT'] = res_path_str
    try:
        from sudachipy import dictionary, tokenizer
        from sudachipy.errors import SudachiError
    except Exception as e:
        raise RuntimeError(f"SudachiPy import failed: {e}")

    res_path = Path(res_path_str)
    # try normal
    try:
        return dictionary.Dictionary().create(), tokenizer
    except Exception:
        pass

    # fallback: make temporary config pointing to absolute system.dic
    sudachi_json = res_path / "sudachi.json"
    system_dic = res_path / "system.dic"
    if sudachi_json.exists() and system_dic.exists():
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
                return dictionary.Dictionary(str(tf.name)).create(), tokenizer
            except Exception:
                importlib.reload(dictionary)
                return dictionary.Dictionary(str(tf.name)).create(), tokenizer
        finally:
            try:
                os.unlink(tf.name)
            except Exception:
                pass
    raise RuntimeError("Sudachi dictionary initialization failed in worker; check resources.")

def _estimate_counter_size_bytes(counter: Counter):
    total = 0
    for k, v in counter.items():
        total += len(k.encode("utf-8")) + 1 + len(str(v)) + 1
    return total

def _flush_counter_to_part(counter: Counter, parts_dir: Path, n: int, worker_tag: str, seq: int):
    """
    counter を辞書順でソートして parts_dir に書き出す（gram\tcount\n）。
    """
    parts_dir.mkdir(parents=True, exist_ok=True)
    path = parts_dir / f"part_{worker_tag}_{seq}.txt"
    with path.open("w", encoding="utf-8") as f:
        for gram in sorted(counter.keys()):
            cnt = counter[gram]
            if cnt > 0:
                f.write(f"{gram}\t{cnt}\n")
    return path

def worker_process(file_list):
    """
    worker entrypoint.
    file_list: list of str paths
    """
    res_path_str = SUDACHI_FULL_RES
    try:
        tok_obj, tokenizer_module = _create_sudachi_tokenizer_local(res_path_str)
    except Exception as e:
        print(f"Worker {os.getpid()} sudachi init failed: {e}", file=sys.stderr)
        return False

    split_mode = tokenizer_module.Tokenizer.SplitMode.B
    pid = os.getpid()
    worker_tag = f"{pid}_{uuid.uuid4().hex[:8]}"
    seqs = {n: 0 for n in range(1, NGRAM_MAX + 1)}
    counters = {n: Counter() for n in range(1, NGRAM_MAX + 1)}
    sizes = {n: 0 for n in range(1, NGRAM_MAX + 1)}
    target_bytes = SIZE_MB * 1024 * 1024

    for file_path in file_list:
        p = Path(file_path)
        if not p.exists():
            continue
        for text in jsonl_iter_texts(p):
            try:
                ms = tok_obj.tokenize(text, split_mode)
            except Exception:
                ms = []
            surfaces = [m.surface() for m in ms if is_japanese_token(m.surface())]
            if not surfaces:
                continue
            L = len(surfaces)
            for n in range(1, min(NGRAM_MAX, L) + 1):
                c = counters[n]
                for i in range(0, L - n + 1):
                    gram = " ".join(surfaces[i:i+n])
                    c[gram] += 1
                sizes[n] = _estimate_counter_size_bytes(c)
                if sizes[n] >= target_bytes:
                    parts_dir = OUT_DIR / "parts" / str(n)
                    _flush_counter_to_part(c, parts_dir, n, worker_tag, seqs[n])
                    seqs[n] += 1
                    counters[n].clear()
                    sizes[n] = 0
    # final flush
    for n in range(1, NGRAM_MAX + 1):
        if counters[n]:
            parts_dir = OUT_DIR / "parts" / str(n)
            _flush_counter_to_part(counters[n], parts_dir, n, worker_tag, seqs[n])
            seqs[n] += 1
            counters[n].clear()
    return True

def _iter_parts_file(path: Path):
    """part file iterator yielding (gram, int(cnt)), assumes file sorted by gram"""
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                gram, cnt = line.rsplit("\t", 1)
                yield (gram, int(cnt))
            except Exception:
                continue

def merge_parts_and_write(n: int):
    parts_dir = OUT_DIR / "parts" / str(n)
    if not parts_dir.exists():
        print(f"No parts for {n}-gram, skipping.")
        return
    part_files = sorted(parts_dir.glob("part_*.txt"))
    if not part_files:
        print(f"No parts for {n}-gram, skipping.")
        return
    # prepare iterators of tuples (gram, count) — they are sorted by gram
    iterators = (_iter_parts_file(p) for p in part_files)
    # heapq.merge will merge by tuple ordering (gram primary)
    merged = heapq.merge(*iterators)
    outp = OUT_DIR / f"{n}hplt0000.txt"
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8") as wf:
        cur = None
        total = 0
        for gram, cnt in merged:
            if cur is None:
                cur = gram
                total = cnt
            elif gram == cur:
                total += cnt
            else:
                if total >= MIN_COUNT:
                    wf.write(f"{cur}\t{total}\n")
                cur = gram
                total = cnt
        if cur is not None and total >= MIN_COUNT:
            wf.write(f"{cur}\t{total}\n")
    print(f"merged -> {outp} (parts: {len(part_files)})")

# Git helpers
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
    msg = f"add parts: {', '.join(rel_paths[:10])}"
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            subprocess.run(["git", "add", "--"] + rel_paths, cwd=str(repo_root), check=True)
            diff = subprocess.run(["git", "diff", "--cached", "--name-only"],
                                  cwd=str(repo_root), check=True, capture_output=True, text=True)
            if not diff.stdout.strip():
                return True
            subprocess.run(["git", "commit", "-m", msg], cwd=str(repo_root), check=True)
            subprocess.run(["git", "push"], cwd=str(repo_root), check=True)
            print(f"git push 成功: {len(rel_paths)} files")
            return True
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "").strip()
            stdout = (e.stdout or "").strip()
            print(f"git 操作失敗 (試行 {attempt}/{retries}): stdout={stdout} stderr={stderr}", file=sys.stderr)
            backoff = min((2 ** attempt) + random.uniform(0, 3), 300)
            time.sleep(backoff)
    print("git push が最大試行回数で失敗しました。手動で確認してください。", file=sys.stderr)
    return False

def watch_and_push(stop_event: threading.Event):
    repo_root = get_git_root(Path.cwd())
    if repo_root is None:
        print("git リポジトリが見つかりません。自動 push を無効化します。")
        return
    seen = set()
    pending = []
    parts_root = OUT_DIR / "parts"
    while not stop_event.is_set():
        new_files = []
        if parts_root.exists():
            for p in sorted(parts_root.rglob("part_*.txt")):
                fp = str(p.resolve())
                if fp not in seen:
                    seen.add(fp)
                    new_files.append(fp)
                    pending.append(fp)
        while len(pending) >= GIT_BATCH:
            to_commit = pending[:GIT_BATCH]
            ok = commit_and_push(to_commit, repo_root)
            if ok:
                pending = pending[GIT_BATCH:]
            else:
                time.sleep(10)
                break
        stop_event.wait(5)
    if pending:
        commit_and_push(pending, repo_root)

def main():
    res_path = Path(SUDACHI_FULL_RES)
    if not res_path.is_dir() or not (res_path / "system.dic").exists():
        print(f"辞書 resources が見つからないか system.dic がありません: {res_path}", file=sys.stderr)
        raise SystemExit(1)
    if not IN_DIR.exists():
        print("入力ディレクトリが存在しません:", IN_DIR, file=sys.stderr)
        raise SystemExit(1)

    files = sorted([str(p) for p in IN_DIR.glob(PATTERN)])
    if not files:
        print("処理対象ファイルが見つかりません。", file=sys.stderr)
        return

    w = min(WORKERS, len(files))
    chunks = [[] for _ in range(w)]
    for i, f in enumerate(files):
        chunks[i % w].append(f)

    print(f"workers={w}, chunks sizes={[len(c) for c in chunks]}")

    (OUT_DIR / "parts").mkdir(parents=True, exist_ok=True)

    # start watcher thread
    stop_event = threading.Event()
    watcher = threading.Thread(target=watch_and_push, args=(stop_event,), daemon=True)
    watcher.start()

    # spawn workers
    try:
        mp.set_start_method('spawn', force=False)
    except Exception:
        pass
    with mp.Pool(processes=w) as pool:
        results = pool.map(worker_process, chunks)
    if not all(results):
        print("一部のワーカーが失敗しました。ログを確認してください。", file=sys.stderr)

    # stop watcher and wait
    stop_event.set()
    watcher.join()

    # merge parts per n
    for n in range(1, NGRAM_MAX + 1):
        merge_parts_and_write(n)

    print("全処理完了。出力ディレクトリ:", OUT_DIR)

if __name__ == "__main__":
    mp.freeze_support()
    main()