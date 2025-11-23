#!/usr/bin/env python3
"""
10_1_????.jsonl などの JSONL (テキストは "text" フィールド) から
テキスト部分だけを抽出して MeCab で形態素解析し、
1〜7 形態素連なりの出現数を集計してファイル出力するスクリプト。

出力:
  gramdata/hplt/data/{n}hplt{index:04d}.txt
各行: "<形態素の連なり> <タブ> <出現回数>"
ファイルは目標サイズ（デフォルト 50MB）ごとにローテーションします。
5 ファイル作成ごとに git add/commit/push を自動で行います（リポジトリ内で実行してください）。

依存:
  pip install mecab-python3
実行例:
  cd d:\gramdata\hplt
  python mecab.py --in . --pattern "10_*.jsonl"
"""
from pathlib import Path
import argparse
import json
import sys
import re
import time
import random
import subprocess
from collections import Counter, defaultdict

try:
    import MeCab
except Exception:
    print("mecab-python3 が必要です: pip install mecab-python3", file=sys.stderr)
    raise SystemExit(1)

# 設定
TARGET_SIZE_MB = 50
MAX_PENDING_PUSH = 5
NGRAM_MAX = 7

# グローバル管理
_pending_files = []
_repo_root = None

def get_git_root(start_path: Path):
    try:
        p = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                           cwd=str(start_path), check=True, capture_output=True, text=True)
        return Path(p.stdout.strip())
    except subprocess.CalledProcessError:
        return None

def commit_and_push(files, repo_root: Path, retries=6):
    if not files:
        return True
    # 相対パスに変換
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
            # 差分がなければ commit をスキップ
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

def _maybe_flush_pending(repo_root):
    global _pending_files
    if repo_root is None:
        return
    if len(_pending_files) >= MAX_PENDING_PUSH:
        to_commit = _pending_files[:]
        _pending_files = []
        commit_and_push(to_commit, repo_root)

def jsonl_iter_texts(path: Path):
    # 1行ごとに JSON を読み、"text" フィールドを yield (空文字はスキップ)
    # ※ 万が一 JSONL でない行があれば簡易抽出を試みる
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
            # try to heuristically extract "text":"..."}
            m = txt_re.search(line)
            if m:
                # find the substring starting index
                idx = m.end()-1
                # try to find the closing quote using simple JSON unescape by finding next occurrence of ", then use json.loads on quoted part
                # safer approach: find the substring from "text": to next ," or "}
                try:
                    # find first colon after "text"
                    # Extract using regex for "text":"( ... )"
                    m2 = re.search(r'"text"\s*:\s*"((?:\\.|[^"\\])*)"', line)
                    if m2:
                        rawtxt = m2.group(1)
                        # unescape
                        t = bytes(rawtxt, "utf-8").decode("unicode_escape")
                        yield t
                        continue
                except Exception:
                    pass
            # fallthrough skip line
            continue

def tokenize_wakati(text, tagger):
    # MeCab の wakati で形態素表層を取得（空白区切り）
    s = tagger.parse(text)
    if not s:
        return []
    # parse may include trailing newline
    tokens = s.strip().split()
    return tokens

def write_counter_to_file(counter: Counter, out_dir: Path, n: int, index: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{n}hplt{index:04d}.txt"
    # sort by count desc then token
    with out_path.open("w", encoding="utf-8") as f:
        for gram, cnt in counter.most_common():
            f.write(f"{gram}\t{cnt}\n")
    return out_path

def estimate_counter_size_bytes(counter: Counter):
    # おおよそのバイトサイズを算出
    total = 0
    for k, v in counter.items():
        total += len(k.encode("utf-8")) + 1 + len(str(v)) + 1
    return total

def process_files(in_dir: Path, pattern: str, out_root: Path, target_mb: int):
    global _repo_root, _pending_files
    target_bytes = target_mb * 1024 * 1024
    # MeCab Tagger（wakati）
    tagger = MeCab.Tagger("-Owakati")
    # counters and indexes per n
    counters = {n: Counter() for n in range(1, NGRAM_MAX+1)}
    indexes = {n: 0 for n in range(1, NGRAM_MAX+1)}
    # track current estimated sizes
    sizes = {n: 0 for n in range(1, NGRAM_MAX+1)}
    out_dir = out_root.resolve()
    file_list = sorted(in_dir.glob(pattern))
    if not file_list:
        print("入力ファイルが見つかりません。", file=sys.stderr)
        return

    for src in file_list:
        print(f"処理中: {src}")
        for text in jsonl_iter_texts(src):
            tokens = tokenize_wakati(text, tagger)
            if not tokens:
                continue
            L = len(tokens)
            # build ngrams
            for n in range(1, min(NGRAM_MAX, L)+1):
                c = counters[n]
                for i in range(0, L - n + 1):
                    gram = " ".join(tokens[i:i+n])
                    c[gram] += 1
                # update estimated size (approx)
                sizes[n] = estimate_counter_size_bytes(c)
            # check rotation per n
            for n in range(1, NGRAM_MAX+1):
                if sizes[n] >= target_bytes and counters[n]:
                    # flush
                    p = write_counter_to_file(counters[n], out_dir, n, indexes[n])
                    indexes[n] += 1
                    _pending_files.append(str(p))
                    counters[n].clear()
                    sizes[n] = 0
                    _maybe_flush_pending(_repo_root)

    # final flush remaining counters
    for n in range(1, NGRAM_MAX+1):
        if counters[n]:
            p = write_counter_to_file(counters[n], out_dir, n, indexes[n])
            indexes[n] += 1
            _pending_files.append(str(p))
            counters[n].clear()
            _maybe_flush_pending(_repo_root)

    # push any remaining pending files
    if _repo_root is not None and _pending_files:
        to_commit = _pending_files[:]
        _pending_files = []
        commit_and_push(to_commit, _repo_root)

def main():
    global _repo_root
    p = argparse.ArgumentParser(description="JSONL を MeCab で解析して 1-7 形態素 ngram を集計・分割出力")
    p.add_argument("--in", "-i", dest="in_dir", default=".", help="入力ディレクトリ（デフォルト: カレント）")
    p.add_argument("--pattern", dest="pattern", default="10_*.jsonl", help='入力ファイルパターン（デフォルト: "10_*.jsonl"）')
    p.add_argument("--out", "-o", dest="out", default="data", help="出力ディレクトリ（デフォルト: data）")
    p.add_argument("--size-mb", dest="size_mb", type=int, default=TARGET_SIZE_MB, help="目標ファイルサイズ MB（デフォルト:50）")
    p.add_argument("--no-git", dest="no_git", action="store_true", help="git commit/push を行わない")
    args = p.parse_args()

    in_dir = Path(args.in_dir).resolve()
    out_root = Path(args.out).resolve()

    if not in_dir.exists():
        print("入力ディレクトリが存在しません。", file=sys.stderr)
        raise SystemExit(1)

    if args.no_git:
        _repo_root = None
    else:
        _repo_root = get_git_root(out_root) or get_git_root(in_dir) or get_git_root(Path.cwd())
        if _repo_root is None:
            print("警告: git リポジトリが見つかりません。自動コミットは無効になります。", file=sys.stderr)

    process_files(in_dir, args.pattern, out_root, args.size_mb)

if __name__ == "__main__":
    main()