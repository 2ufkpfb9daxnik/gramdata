#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
purif/ 以下のファイルを順に読み、行（1 text = 1行）から
EXCLUDE_WORDS_2 に含まれるワードを部分一致で除外して出力します。

出力は OUT_DIR/purif2{index:04d}.txt。1ファイル上限 SIZE_MB（MB）。
1出力ファイルが満たない場合は次の入力ファイルの行も続けて書き込みます。
各入力ファイルは「正常に最後まで処理完了」したら削除されます。
除外語リストはこのファイル内の EXCLUDE_WORDS_2 に埋め込みます。

コマンドラインで入力フォルダ等を上書きできます:
  python purif2.py --input-dir D:\path\to\purif --out-dir D:\path\to\purif2 --size-mb 50
"""
import sys
import os
import argparse
from pathlib import Path
import re

# --- デフォルト設定（必要に応じて編集） ---
IN_DIR = Path(r"D:\gramdata\hplt\purif7")        # デフォルト入力ディレクトリ（purif.py の出力）
PATTERN = "purif*.txt"
OUT_DIR = Path(r"D:\gramdata\hplt\purif8")      # デフォルト出力ディレクトリ
SIZE_MB = 50
ENCODING = "utf-8"
CASE_INSENSITIVE = False

# 除外語（ここに除外したい単語・フレーズを追加）
EXCLUDE_WORDS_2 = [
    # 例:
    # "ノイズワード1",
    # "広告ドメイン.example",
    # "包茎治療", "樋口総合法律事務所", "ホテヘル", "スカトロ趣味", "外壁塗装", "PCMAX", 
    # "今だけ全額返金保証付きのチャップアップ！", "超乳な女性と出会いたい男性", "レディース", "引っ越し",
    # "保育士", "下取り", "ここではセフレと出会うためにオススメな出会い系を紹介しています。",
    # "の恋愛", "出会い", "学生時代にJump＆Jiveを好きになり", "畳表", "バイク見積もり",
    # "脱毛", "転職", 
    # "トイレリフォーム", "期間工", "楽天市場", 
    # "リフォーム", "デリヘル", 
    # "自己破産","探偵", "商品番号",
    # "ジュニアアイドルから着エロ、インディーズ系動画までアイドル動画なら「いちごキャンディ」！", "水道屋" 
    "育毛"
]
# -------------------------------

_ws_re = re.compile(r'\s+')

def normalize_line(s: str) -> str:
    s = s.replace("\r", " ").replace("\n", " ")
    s = _ws_re.sub(" ", s).strip()
    return s

def gather_input_files(indir: Path, pattern: str):
    if not indir.exists():
        return []
    return sorted(indir.glob(pattern))

def process_files(files, out_dir: Path, size_mb: int, exclude_words, case_insensitive: bool):
    out_dir.mkdir(parents=True, exist_ok=True)
    target = size_mb * 1024 * 1024
    idx = 0
    f = None
    bytes_written = 0
    created = []
    total_written = 0
    total_skipped = 0

    excl = [w.lower() for w in exclude_words] if case_insensitive else list(exclude_words)

    def open_new():
        nonlocal f, bytes_written, idx
        if f:
            f.close()
            created.append(out_dir / f"purif8{idx:04d}.txt")
        path = out_dir / f"purif8{idx:04d}.txt"
        f = path.open("w", encoding=ENCODING)
        bytes_written = 0

    open_new()

    for src in files:
        print("processing:", src.name)
        try:
            with src.open("r", encoding=ENCODING, errors="replace") as rf:
                for raw in rf:
                    line = normalize_line(raw)
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
                    f.write(out_line)
                    bytes_written += b
                    total_written += 1
            # finished this input file successfully -> delete it
            try:
                src.unlink()
                print(f"removed source: {src.name}")
            except Exception as e:
                print(f"warning: failed to remove {src.name}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"error processing {src.name}: {e}", file=sys.stderr)
            # do not delete on error, continue to next file
            continue

    if f:
        f.close()
        created.append(out_dir / f"purif8{idx:04d}.txt")

    return created, total_written, total_skipped

def parse_args():
    p = argparse.ArgumentParser(description="purif7/ -> purif8/ filtering tool")
    p.add_argument("--input-dir", "-i", type=str, help="入力ディレクトリ（purif 出力）")
    p.add_argument("--out-dir", "-o", type=str, help="出力ディレクトリ")
    p.add_argument("--pattern", "-p", type=str, help="入力ファイルパターン（glob）")
    p.add_argument("--size-mb", "-s", type=int, help="出力ファイル分割サイズ（MB）")
    p.add_argument("--case-insensitive", action="store_true", help="除外ワードを大文字小文字無視で判定する")
    return p.parse_args()

def main():
    args = parse_args()
    indir = Path(args.input_dir) if args.input_dir else IN_DIR
    outdir = Path(args.out_dir) if args.out_dir else OUT_DIR
    pattern = args.pattern if args.pattern else PATTERN
    size_mb = args.size_mb if args.size_mb else SIZE_MB
    case_ins = args.case_insensitive or CASE_INSENSITIVE

    files = gather_input_files(indir, pattern)
    if not files:
        print("対象ファイルが見つかりません:", indir / pattern, file=sys.stderr)
        return
    print(f"入力ファイル数: {len(files)}")
    print(f"除外ワード数: {len(EXCLUDE_WORDS_2)} (このスクリプトに埋め込み済み)")

    created, written, skipped = process_files(files, outdir, size_mb, EXCLUDE_WORDS_2, case_ins)
    print("完了。出力先:", outdir)
    print("生成ファイル数:", len(created))
    print("合計書き出し行数:", written)
    print("除外/空行でスキップした行数:", skipped)
    print("ファイル一覧:")
    for p in created:
        try:
            sz = p.stat().st_size
        except Exception:
            sz = 0
        print(f" - {p.name} ({sz} bytes)")

if __name__ == "__main__":
    main()