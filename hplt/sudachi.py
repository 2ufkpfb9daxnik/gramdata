"""
purif8/*.txt を読み、Sudachi (sudachidict_full) で形態素解析して
1..7-gram を厳密に集計（SQLite 不使用）。

このバージョンは「入力ファイルは削除しない」設定です（処理後も元ファイルを保持します）。
その他の動作は以前のスクリプトと同様で、チャンク→マージ→出現頻度降順ソート→最終出力、
最終出力ファイルが N_FILES_PER_PUSH 個たまるごとに git add/commit/push を行います。
"""
import os
import sys
import json
import tempfile
import heapq
import subprocess
import time
import random
from pathlib import Path
from collections import Counter
import shutil
import re

# --- 設定 ---
SUDACHI_FULL_RES = r'D:\gramdata\.venv\Lib\site-packages\sudachidict_full\resources'
IN_DIR = Path(r"D:\gramdata\hplt\purif8")
PATTERN = "purif*.txt"
OUT_DIR = Path(r"D:\gramdata\hplt\word")
CHUNKS_DIR = OUT_DIR / "chunks"
MIN_COUNT = 10            # 出力に含める最低頻度
NGRAM_MAX = 7
CHUNK_MAX_MB = 50         # インメモリ Counter をフラッシュするサイズ目安（MB）
CHUNK_SORT_MB = 200       # 集計済ファイルを出現頻度で外部ソートするときのチャンクサイズ（MB）
SIZE_MB = 50              # 最終出力ファイルの分割サイズ（MB）
MAX_OPEN_FILES = 100      # マージ時の同時オープンするチャンク数の目安

# Git push 関連
N_FILES_PER_PUSH = 5
GIT_COMMIT_MESSAGE_PREFIX = "add ngram files"
# -----------------

os.environ["SUDACHIPY_DICT"] = SUDACHI_FULL_RES
try:
    from sudachipy import dictionary, tokenizer
    from sudachipy.errors import SudachiError
except Exception as e:
    print("SudachiPy import error; SUDACHI_FULL_RES を確認してください:", e, file=sys.stderr)
    raise SystemExit(1)

# 日本語判定
_JP_RE = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\u3000-\u303F\uFF00-\uFFEF\u2010-\u2015]')
def is_japanese_token(s: str) -> bool:
    return bool(_JP_RE.search(s))

def _create_tokenizer(res_path: Path):
    try:
        return dictionary.Dictionary().create()
    except Exception:
        pass
    sudachi_json = res_path / "sudachi.json"
    system_dic = res_path / "system.dic"
    if sudachi_json.exists() and system_dic.exists():
        import importlib
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
            tf.flush(); tf.close()
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

def estimate_counter_bytes(counter: Counter):
    total = 0
    for k, v in counter.items():
        total += len(k.encode("utf-8")) + 1 + len(str(v)) + 1
    return total

def flush_counter_to_chunk(counter: Counter, n: int, chunks_dir: Path, idx: int):
    chunks_dir.mkdir(parents=True, exist_ok=True)
    path = chunks_dir / f"{n}chunk{idx:04d}.tsv"
    with path.open("w", encoding="utf-8") as wf:
        for gram, cnt in sorted(counter.items()):
            wf.write(f"{gram}\t{cnt}\n")
    return path

def merge_sorted_files(file_paths, out_path):
    """複数のキー（gram）ソート済みチャンクをマージして合算済みファイルを作る（gram順）"""
    iters = []
    files = []
    try:
        for p in file_paths:
            f = Path(p).open("r", encoding="utf-8", errors="replace")
            files.append(f)
            def gen(fh):
                for ln in fh:
                    ln = ln.rstrip("\n")
                    if not ln:
                        continue
                    g, c = ln.rsplit("\t", 1)
                    yield (g, int(c))
            iters.append(gen(f))
        merged = heapq.merge(*iters, key=lambda x: x[0])
        with out_path.open("w", encoding="utf-8") as wf:
            cur_g = None
            cur_sum = 0
            for g, c in merged:
                if cur_g is None:
                    cur_g = g; cur_sum = c
                elif g == cur_g:
                    cur_sum += c
                else:
                    wf.write(f"{cur_g}\t{cur_sum}\n")
                    cur_g = g; cur_sum = c
            if cur_g is not None:
                wf.write(f"{cur_g}\t{cur_sum}\n")
    finally:
        for f in files:
            try:
                f.close()
            except Exception:
                pass

def multi_pass_merge(paths):
    """paths をバッチに分けて順次マージし、最終的に一つの合算ファイルを返す（Path）"""
    if not paths:
        return None
    cur_list = [Path(p) for p in paths]
    round_idx = 0
    while len(cur_list) > 1:
        new_list = []
        for i in range(0, len(cur_list), MAX_OPEN_FILES):
            batch = cur_list[i:i+MAX_OPEN_FILES]
            tmp = CHUNKS_DIR / f"merge_r{round_idx}_{i:04d}.tsv"
            merge_sorted_files(batch, tmp)
            new_list.append(tmp)
            for p in batch:
                try:
                    p.unlink()
                except Exception:
                    pass
        cur_list = new_list
        round_idx += 1
    return cur_list[0]

# -----------------------
# 出現頻度での外部ソート（メモリに乗らない場合に対応）
# -----------------------
def external_sort_agg_by_count(agg_path: Path, out_sorted_path: Path, temp_dir: Path, chunk_mb: int):
    """
    agg_path (gram\tcount\n のファイル) を "count desc, gram asc" でソートして out_sorted_path に書く。
    アルゴリズム: 入力を chunk_mb 毎に読み込んでメモリソート -> チャンクを書き出し -> k-way マージ。
    """
    temp_dir.mkdir(parents=True, exist_ok=True)
    chunk_limit = chunk_mb * 1024 * 1024
    chunk_paths = []
    buf = []
    buf_bytes = 0
    idx = 0

    with agg_path.open("r", encoding="utf-8", errors="replace") as rf:
        for ln in rf:
            ln = ln.rstrip("\n")
            if not ln:
                continue
            try:
                gram, cnts = ln.rsplit("\t", 1)
                cnt = int(cnts)
            except Exception:
                continue
            entry = (cnt, gram)
            buf.append(entry)
            buf_bytes += len(ln.encode("utf-8")) + 8
            if buf_bytes >= chunk_limit:
                # sort chunk by (-count, gram)
                buf.sort(key=lambda x: (-x[0], x[1]))
                cp = temp_dir / f"sort_chunk_{idx:04d}.tsv"
                with cp.open("w", encoding="utf-8") as wf:
                    for c, g in buf:
                        wf.write(f"{c}\t{g}\n")
                chunk_paths.append(cp)
                idx += 1
                buf = []
                buf_bytes = 0
    # flush remaining
    if buf:
        buf.sort(key=lambda x: (-x[0], x[1]))
        cp = temp_dir / f"sort_chunk_{idx:04d}.tsv"
        with cp.open("w", encoding="utf-8") as wf:
            for c, g in buf:
                wf.write(f"{c}\t{g}\n")
        chunk_paths.append(cp)
        idx += 1
        buf = []
    # 単一チャンクなら変換して終わり
    if not chunk_paths:
        # nothing to do
        out_sorted_path.unlink(missing_ok=True)
        agg_path.replace(out_sorted_path)
        return out_sorted_path
    if len(chunk_paths) == 1:
        # read chunk and write as gram\tcount
        with chunk_paths[0].open("r", encoding="utf-8", errors="replace") as rf, \
             out_sorted_path.open("w", encoding="utf-8") as wf:
            for ln in rf:
                ln = ln.rstrip("\n")
                if not ln:
                    continue
                c, g = ln.split("\t", 1)
                wf.write(f"{g}\t{c}\n")
        try:
            chunk_paths[0].unlink()
            temp_dir.rmdir()
        except Exception:
            pass
        return out_sorted_path

    # k-way merge chunk_paths (each line: count\tgram), merge by (-count, gram)
    files = []
    heap = []
    try:
        for i, p in enumerate(chunk_paths):
            f = p.open("r", encoding="utf-8", errors="replace")
            files.append(f)
            ln = f.readline()
            if not ln:
                continue
            ln = ln.rstrip("\n")
            c_str, g = ln.split("\t", 1)
            c = int(c_str)
            heapq.heappush(heap, (-c, g, i))
        with out_sorted_path.open("w", encoding="utf-8") as wf:
            while heap:
                negc, g, i = heapq.heappop(heap)
                c = -negc
                wf.write(f"{g}\t{c}\n")
                fh = files[i]
                ln = fh.readline()
                if ln:
                    ln = ln.rstrip("\n")
                    c_str, g2 = ln.split("\t", 1)
                    c2 = int(c_str)
                    heapq.heappush(heap, (-c2, g2, i))
    finally:
        for f in files:
            try:
                f.close()
            except Exception:
                pass
        # cleanup chunks
        for p in chunk_paths:
            try:
                p.unlink()
            except Exception:
                pass
        try:
            temp_dir.rmdir()
        except Exception:
            pass
    return out_sorted_path

def export_sorted_to_outputs(sorted_agg_path: Path, n: int, out_dir: Path, min_count: int, size_mb: int):
    """
    sorted_agg_path: gram\tcount lines sorted by count desc then gram asc.
    出力を SIZE_MB ごとに分割して書く。戻りは生成したファイルのリスト。
    """
    created = []
    target = size_mb * 1024 * 1024
    idx = 0
    f = None
    bytes_written = 0
    with sorted_agg_path.open("r", encoding="utf-8", errors="replace") as rf:
        for ln in rf:
            ln = ln.rstrip("\n")
            if not ln:
                continue
            gram, cnts = ln.rsplit("\t", 1)
            cnt = int(cnts)
            if cnt < min_count:
                continue
            line = f"{gram}\t{cnt}\n"
            b = len(line.encode("utf-8"))
            if f is None:
                path = out_dir / f"{n}hplt{idx:04d}.txt"
                f = path.open("w", encoding="utf-8")
                bytes_written = 0
            if bytes_written + b > target and bytes_written > 0:
                f.close()
                created.append(path)
                idx += 1
                path = out_dir / f"{n}hplt{idx:04d}.txt"
                f = path.open("w", encoding="utf-8")
                bytes_written = 0
            f.write(line)
            bytes_written += b
    if f is not None:
        f.close()
        created.append(out_dir / f"{n}hplt{idx:04d}.txt")
    return created

# Git helpers (same as before)
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
            diff = subprocess.run(["git", "diff", "--cached", "--name-only"], cwd=str(repo_root),
                                  check=True, capture_output=True, text=True)
            if not diff.stdout.strip():
                return True
            subprocess.run(["git", "commit", "-m", msg], cwd=str(repo_root), check=True)
            subprocess.run(["git", "push"], cwd=str(repo_root), check=True)
            return True
        except subprocess.CalledProcessError:
            time.sleep(min((2 ** attempt) + random.random(), 60))
    return False

# -----------------------
# メイン処理
# -----------------------
def process_inputs():
    res_path = Path(SUDACHI_FULL_RES)
    if not res_path.is_dir() or not (res_path / "system.dic").exists():
        print("辞書 resources が見つからないか system.dic がありません:", res_path, file=sys.stderr)
        return
    if not IN_DIR.exists():
        print("入力ディレクトリが存在しません:", IN_DIR, file=sys.stderr)
        return
    files = sorted(IN_DIR.glob(PATTERN))
    if not files:
        print("処理対象ファイルが見つかりません。", file=sys.stderr)
        return

    tok = _create_tokenizer(res_path)
    split_mode = tokenizer.Tokenizer.SplitMode.B

    # counters per n
    counters = {n: Counter() for n in range(1, NGRAM_MAX+1)}
    sizes = {n: 0 for n in range(1, NGRAM_MAX+1)}
    chunk_idx = {n: 0 for n in range(1, NGRAM_MAX+1)}
    chunk_paths = {n: [] for n in range(1, NGRAM_MAX+1)}
    chunk_target = CHUNK_MAX_MB * 1024 * 1024

    for src in files:
        print("processing", src.name)
        try:
            with src.open("r", encoding="utf-8", errors="replace") as rf:
                for line in rf:
                    text = line.rstrip("\n")
                    if not text:
                        continue
                    try:
                        ms = tok.tokenize(text, split_mode)
                    except Exception:
                        ms = []
                    surfaces = [m.surface() for m in ms if is_japanese_token(m.surface())]
                    if not surfaces:
                        continue
                    L = len(surfaces)
                    for n in range(1, min(NGRAM_MAX, L)+1):
                        c = counters[n]
                        for i in range(0, L-n+1):
                            gram = " ".join(surfaces[i:i+n])
                            c[gram] += 1
                        sizes[n] = estimate_counter_bytes(c)
                        if sizes[n] >= chunk_target:
                            p = flush_counter_to_chunk(c, n, CHUNKS_DIR, chunk_idx[n])
                            chunk_paths[n].append(p)
                            chunk_idx[n] += 1
                            c.clear()
                            sizes[n] = 0
            # NOTE: do NOT delete source file; keep original files intact
            print(f"processed (kept): {src.name}")
        except Exception as e:
            print(f"error processing {src.name}: {e}", file=sys.stderr)
            continue

    # flush remaining counters
    for n in range(1, NGRAM_MAX+1):
        c = counters[n]
        if c:
            p = flush_counter_to_chunk(c, n, CHUNKS_DIR, chunk_idx[n])
            chunk_paths[n].append(p)
            chunk_idx[n] += 1
            c.clear()

    # merge chunks per n, sort by count desc, export, and git-push in batches
    repo_root = _get_git_root(OUT_DIR) or _get_git_root(Path.cwd())
    push_batch = []
    all_created = []
    for n in range(1, NGRAM_MAX+1):
        paths = [p for p in chunk_paths[n] if p.exists()]
        if not paths:
            print(f"no chunks for {n}-gram")
            continue
        print(f"merging {len(paths)} chunks for {n}-gram ...")
        merged = multi_pass_merge(paths)
        if merged is None:
            continue
        # external sort by count (creates sorted_agg file)
        sorted_dir = CHUNKS_DIR / f"sort_n{n}"
        sorted_dir.mkdir(parents=True, exist_ok=True)
        sorted_agg = CHUNKS_DIR / f"merged_n{n}_sorted.tsv"
        print(f"sorting aggregated counts by frequency for {n}-gram ...")
        external_sort_agg_by_count(Path(merged), sorted_agg, sorted_dir, CHUNK_SORT_MB)
        try:
            Path(merged).unlink()
        except Exception:
            pass
        # export frequency-sorted aggregated results to final outputs
        print(f"exporting final files for {n}-gram ...")
        created = export_sorted_to_outputs(sorted_agg, n, OUT_DIR, MIN_COUNT, SIZE_MB)
        all_created.extend(created)
        # remove sorted aggregated file
        try:
            Path(sorted_agg).unlink()
        except Exception:
            pass
        # git batching
        for cp in created:
            push_batch.append(cp)
            if len(push_batch) >= N_FILES_PER_PUSH and repo_root:
                _git_add_commit_push(push_batch, repo_root)
                push_batch = []

    # push remaining files
    if push_batch and repo_root:
        _git_add_commit_push(push_batch, repo_root)
        push_batch = []

    # cleanup chunks dir if empty
    try:
        if CHUNKS_DIR.exists() and not any(CHUNKS_DIR.iterdir()):
            CHUNKS_DIR.rmdir()
    except Exception:
        pass

    print("done. outputs:", OUT_DIR)
    print("created files:", len(all_created))

def main():
    process_inputs()

if __name__ == "__main__":
    main()
