#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jsonl ファイル群から text 要素のみを抽出してプレーンテキストにするスクリプト。

- 入力: IN_DIR / PATTERN の jsonl ファイル群
- 出力: OUT_DIR / purif{index:04d}.txt（1行 = 1 text 要素）
- 1ファイルの上限は SIZE_MB（MB）。超えたら次のインデックスに分割。
- 除外ワードはこのファイル内の EXCLUDE_WORDS に列挙（部分一致で除外）。
- 各入力ファイルは「完全に処理完了」したら削除されます。
"""
import os
import sys
import json
import re
from pathlib import Path

# ...existing code...
# --- 設定（必要に応じて編集） ---
IN_DIR = Path(r"D:\gramdata\hplt\data")
PATTERN = "*.jsonl"
OUT_DIR = Path(r"D:\gramdata\hplt\purif")
SIZE_MB = 50
ENCODING = "utf-8"

# 除外語リスト（ここにノイズワードを追加してください。部分一致で除外されます）
# 例: EXCLUDE_WORDS = ["casino", "ビデオスロッツ", "広告ドメイン"]
EXCLUDE_WORDS = [
    # ここに除外ワードを追加
    # "ノイズワード1",
    # "ノイズワード2",
    "カジノ", "パチンコ", "仮想通貨", "セクキャバ", "オナクラ", "入金", 
]

# 大文字小文字を無視したい場合は True（英数字が混じる行に有効）
CASE_INSENSITIVE_EXCLUDE = False
# -------------------------------

# 正規表現での text 要素抽出（簡易）
_txt_re = re.compile(r'"text"\s*:\s*"((?:\\.|[^"\\])*)"')
# 空白正規化
_ws_re = re.compile(r'\s+')

def jsonl_iter_texts(path: Path):
    """
    path の jsonl を1行ずつ読み、text 要素を取り出して yield する。
    JSON パースがうまくいかない場合は簡易 regex による抽出を試みる。
    """
    for raw in path.open("r", encoding=ENCODING, errors="replace"):
        line = raw.rstrip("\n")
        if not line:
            continue
        # まず普通に JSON として parse
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                t = obj.get("text") or obj.get("body") or obj.get("content")
                if t is not None:
                    yield t
                    continue
        except Exception:
            # fallthrough to regex extraction
            pass
        # regex fallback (handles \" escapes)
        m = _txt_re.search(line)
        if m:
            rawtxt = m.group(1)
            try:
                t = bytes(rawtxt, "utf-8").decode("unicode_escape")
                yield t
            except Exception:
                try:
                    t2 = rawtxt.encode("utf-8").decode("unicode_escape", errors="ignore")
                    yield t2
                except Exception:
                    continue

def normalize_line(text: str) -> str:
    # 改行をスペースにし、連続空白を単一スペースに、両端トリム
    s = text.replace("\r", " ").replace("\n", " ")
    s = _ws_re.sub(" ", s).strip()
    return s

def write_texts(files, out_dir: Path, size_mb: int, exclude_words, case_insensitive: bool):
    out_dir.mkdir(parents=True, exist_ok=True)
    target = size_mb * 1024 * 1024
    idx = 0
    f = None
    bytes_written = 0
    created = []
    total_lines = 0
    total_skipped = 0

    # prepare exclude matching
    if case_insensitive:
        excl = [w.lower() for w in exclude_words]
    else:
        excl = list(exclude_words)

    def open_new():
        nonlocal f, bytes_written, idx
        if f:
            f.close()
            created.append(out_dir / f"purif{idx:04d}.txt")
        path = out_dir / f"purif{idx:04d}.txt"
        f = path.open("w", encoding=ENCODING)
        bytes_written = 0

    # start with first file
    open_new()

    try:
        for src in files:
            print("processing:", src.name)
            # ファイル単位で try/except を設け、処理完了時のみ削除する
            try:
                for txt in jsonl_iter_texts(src):
                    line = normalize_line(txt)
                    if not line:
                        total_skipped += 1
                        continue
                    hay = line.lower() if case_insensitive else line
                    skip = False
                    for w in excl:
                        if not w:
                            continue
                        if w in hay:
                            skip = True
                            break
                    if skip:
                        total_skipped += 1
                        continue
                    out_line = line + "\n"
                    b = len(out_line.encode(ENCODING))
                    # rotate if adding this line would exceed target and current file not empty
                    if bytes_written + b > target and bytes_written > 0:
                        idx += 1
                        open_new()
                    # write (even if b > target when file empty, we still write)
                    f.write(out_line)
                    bytes_written += b
                    total_lines += 1
                # inner loop completed normally -> safe to delete source file
                try:
                    src.unlink()
                    print(f"removed source: {src.name}")
                except Exception as e:
                    print(f"warning: failed to remove {src.name}: {e}", file=sys.stderr)
            except Exception as e_file:
                # 処理中に例外が起きた場合はその入力ファイルは削除しない
                print(f"error processing {src.name}: {e_file}", file=sys.stderr)
                # 続行して次ファイルへ
                continue
    finally:
        if f:
            f.close()
            created.append(out_dir / f"purif{idx:04d}.txt")
    return created, total_lines, total_skipped

def main():
    if not IN_DIR.exists():
        print("入力ディレクトリが存在しません:", IN_DIR, file=sys.stderr)
        return
    files = sorted(IN_DIR.glob(PATTERN))
    if not files:
        print("対象ファイルが見つかりません:", IN_DIR / PATTERN, file=sys.stderr)
        return

    exclude_words = EXCLUDE_WORDS
    print(f"除外ワード数: {len(exclude_words)} (ファイル内の EXCLUDE_WORDS を編集してください)")
    created, n_written, n_skipped = write_texts(files, OUT_DIR, SIZE_MB, exclude_words, CASE_INSENSITIVE_EXCLUDE)
    print("完了。出力先:", OUT_DIR)
    print("生成ファイル数:", len(created))
    print("合計書き出し行数:", n_written)
    print("除外/空行でスキップした行数:", n_skipped)
    print("ファイル一覧:")
    for p in created:
        try:
            sz = p.stat().st_size
        except Exception:
            sz = 0
        print(f" - {p} ({sz} bytes)")

if __name__ == "__main__":
    main()