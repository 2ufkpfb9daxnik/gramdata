"""
nwc2010/nwc2010-ngrams 以下の .xz ファイルを解凍し、
テキストをほぼ 50MB ごとに分割して
nwc2010/{kind}{over}{gms}_{4桁}.txt という名前で出力するスクリプト。

使い方:
  python unpack.py            # カレントを d:\gramdata\nwc2010 にして実行する想定
  python unpack.py --in ROOT --out OUTROOT --size-mb 50

仕様メモ:
 - 入力ファイル例:
   nwc2010-ngrams/word/over99/2gms/2gm-0001.xz
 - 出力ファイル例:
   <OUTROOT>/wordover99 2gms_0000.txt  (ただし空白は自動で削除 -> wordover992gms_0000.txt)
"""
from pathlib import Path
import argparse
import lzma
import os
import sys

def find_xz_files(root: Path):
    return root.rglob("*.xz")

def make_out_basename(xz_path: Path):
    parts = xz_path.parts
    # try to locate "nwc2010-ngrams" in path and take following components
    try:
        idx = parts.index("nwc2010-ngrams")
        rel = parts[idx+1:]
    except ValueError:
        # fallback: use last 4 components if available
        rel = parts[-4:]
    # rel expected: [kind, over..., gmsdir, filename]
    if len(rel) >= 3:
        kind = rel[0]
        over = rel[1]
        gmsdir = rel[2]
    else:
        # fallback to pieces from filename
        kind = rel[0] if len(rel) > 0 else "unknown"
        over = rel[1] if len(rel) > 1 else ""
        gmsdir = rel[2] if len(rel) > 2 else ""
    # sanitize components (remove spaces)
    kind = kind.replace(" ", "")
    over = over.replace(" ", "")
    gmsdir = gmsdir.replace(" ", "")
    base = f"{kind}{over}{gmsdir}"
    return base

def ensure_dir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def unpack_and_split(xz_path: Path, out_root: Path, max_bytes: int, force: bool=False):
    base = make_out_basename(xz_path)
    # open xz as text stream (utf-8, replace errors)
    try:
        src = lzma.open(xz_path, mode="rt", encoding="utf-8", errors="replace")
    except Exception as e:
        print(f"解凍失敗: {xz_path} -> {e}", file=sys.stderr)
        return False

    idx = 0
    out_fp = None
    bytes_written = 0
    written_any = False
    try:
        for line in src:
            b = line.encode("utf-8")
            if out_fp is None:
                out_name = f"{base}_{idx:04d}.txt"
                out_path = out_root / out_name
                if out_path.exists() and not force:
                    # そのファイルが既にあれば上書きするかスキップするかを選べるがここでは上書きする
                    pass
                ensure_dir(out_path)
                out_fp = open(out_path, "wb")
                bytes_written = 0
            # ローテーション条件: 現在のファイルに行を書いた後に max_bytes を超える場合は
            # 新しいファイルに切り替える。ただし最初の行は必ず書く。
            if bytes_written + len(b) > max_bytes and bytes_written > 0:
                out_fp.close()
                idx += 1
                out_name = f"{base}_{idx:04d}.txt"
                out_path = out_root / out_name
                ensure_dir(out_path)
                out_fp = open(out_path, "wb")
                bytes_written = 0
            out_fp.write(b)
            bytes_written += len(b)
            written_any = True
    finally:
        if out_fp is not None:
            out_fp.close()
        src.close()

    print(f"完了: {xz_path} -> {base} (分割ファイル数: {idx+1 if written_any else 0})")
    return True

def main():
    parser = argparse.ArgumentParser(description="nwc2010 の .xz を解凍して約 50MB ごとに分割する")
    parser.add_argument("--in", dest="in_root", default="nwc2010-ngrams",
                        help="入力ルートディレクトリ (デフォルト: nwc2010-ngrams)")
    parser.add_argument("--out", dest="out_root", default=".",
                        help="出力ルートディレクトリ (デフォルト: カレント、通常は nwc2010 を指定)")
    parser.add_argument("--size-mb", dest="size_mb", type=int, default=50,
                        help="1ファイルあたりの目標サイズ (MB, デフォルト:50)")
    parser.add_argument("--force", action="store_true", help="既存出力を上書きする")
    args = parser.parse_args()

    in_root = Path(args.in_root)
    out_root = Path(args.out_root)
    max_bytes = args.size_mb * 1024 * 1024

    if not in_root.exists():
        print(f"入力ディレクトリが見つかりません: {in_root}", file=sys.stderr)
        sys.exit(1)

    for xz in find_xz_files(in_root):
        try:
            unpack_and_split(xz, out_root, max_bytes, force=args.force)
        except Exception as e:
            print(f"処理中に例外: {xz} -> {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
