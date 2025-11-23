"""
10_1.jsonl.zst のような .zst (zstd) 圧縮された JSONL を解凍し、
テキストを約 50MB ごとに分割して出力するスクリプト。

出力名のルール:
 - 入力: 10_1.jsonl.zst
 - 出力: 10_1_0000.jsonl, 10_1_0001.jsonl, ...

実行場所: d:\gramdata\hplt での実行を想定（引数でパス指定可）
依存: python -m pip install zstandard
"""
from pathlib import Path
import argparse
import io
import sys

try:
    import zstandard as zstd
except Exception:
    print("zstandard が必要です: pip install zstandard", file=sys.stderr)
    raise SystemExit(1)


def split_zst_jsonl(src_path: Path, out_dir: Path, size_mb: int = 50, force: bool = False):
    max_bytes = size_mb * 1024 * 1024
    src_path = src_path.resolve()
    if not src_path.exists():
        raise FileNotFoundError(src_path)

    # base name without .zst
    base = src_path.name
    if base.endswith(".zst"):
        base = base[:-4]
    # if base ends with .jsonl, keep extension separate
    if base.endswith(".jsonl"):
        name_root = base[:-6]
        ext = ".jsonl"
    else:
        name_root = base
        ext = ""

    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    decompressor = zstd.ZstdDecompressor()

    idx = 0
    out_fp = None
    bytes_written = 0
    written_files = []

    try:
        with src_path.open("rb") as compressed:
            with decompressor.stream_reader(compressed) as reader:
                # Text wrapper to iterate lines safely (UTF-8, replace errors)
                text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="replace", newline="")
                for line in text_stream:
                    data = line.encode("utf-8")
                    if out_fp is None:
                        out_name = f"{name_root}_{idx:04d}{ext}"
                        out_path = out_dir / out_name
                        if out_path.exists() and not force:
                            # 上書きしたくない場合は進めるがここでは上書きを許可する
                            pass
                        out_fp = out_path.open("wb")
                        bytes_written = 0
                    # ローテーションは行単位で行い、行を途中で切らない
                    if bytes_written + len(data) > max_bytes and bytes_written > 0:
                        out_fp.close()
                        written_files.append(str(out_path))
                        idx += 1
                        out_name = f"{name_root}_{idx:04d}{ext}"
                        out_path = out_dir / out_name
                        out_fp = out_path.open("wb")
                        bytes_written = 0
                    out_fp.write(data)
                    bytes_written += len(data)
    finally:
        if out_fp is not None:
            out_fp.close()
            # add last file if any
            try:
                written_files.append(str(out_path))
            except Exception:
                pass

    print(f"完了: {src_path.name} -> {len(written_files)} ファイル ({size_mb}MB 目安)")
    for p in written_files:
        try:
            s = Path(p).stat().st_size
            print(f"  {p}  {s} bytes")
        except Exception:
            print(f"  {p}")
    return written_files


def main():
    p = argparse.ArgumentParser(description="zst 圧縮 JSONL を解凍して約指定MBごとに分割する")
    p.add_argument("src", nargs="?", default="three/sorted/jpn_Jpan/10_1.jsonl.zst", help="入力 .zst ファイル（デフォルト: 10_1.jsonl.zst）")
    p.add_argument("--out", "-o", default=".", help="出力ディレクトリ（デフォルト: カレント）")
    p.add_argument("--size-mb", type=int, default=50, help="1ファイルあたりの目標サイズ (MB, デフォルト:50)")
    p.add_argument("--force", action="store_true", help="既存ファイルを上書きする")
    args = p.parse_args()

    src = Path(args.src)
    out = Path(args.out)

    try:
        split_zst_jsonl(src, out, size_mb=args.size_mb, force=args.force)
    except Exception as e:
        print(f"エラー: {e}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()