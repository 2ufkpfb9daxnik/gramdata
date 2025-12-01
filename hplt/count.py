#!/usr/bin/env python3
"""
指定ディレクトリ内のファイル群について
- バイト合計
- 文字（Unicode code points）合計
- ファイル数、平均バイト/文字
を表示する簡易スクリプト。

使い方（PowerShell）:
python d:\gramdata\hplt\count.py --dir D:\gramdata\hplt\purif --pattern purif*.txt
"""
import argparse
from pathlib import Path

def analyze(files):
    total_bytes = 0
    total_chars = 0
    file_count = 0
    largest = []

    for p in files:
        try:
            b = p.stat().st_size
        except Exception:
            continue
        # 文字数はテキストとして読み込み（utf-8 で失敗した箇所は置換）
        chars = 0
        try:
            with p.open("r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    chars += len(line)
        except Exception:
            # 読めない場合はバイト数を仮の文字数として扱う
            chars = b
        total_bytes += b
        total_chars += chars
        file_count += 1
        largest.append((b, chars, p))
    largest.sort(reverse=True, key=lambda x: x[0])
    return total_bytes, total_chars, file_count, largest

def fmt(n):
    for unit in ("","K","M","G","T"):
        if abs(n) < 1024.0:
            return f"{n:.2f}{unit}"
        n /= 1024.0
    return f"{n:.2f}P"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", "-d", required=True, help="対象ディレクトリ")
    ap.add_argument("--pattern", "-p", default="*", help="glob パターン（例: purif*.txt）")
    ap.add_argument("--top", "-t", type=int, default=10, help="大きいファイル上位表示数")
    args = ap.parse_args()

    indir = Path(args.dir)
    if not indir.exists():
        print("ディレクトリが見つかりません:", indir)
        return

    files = sorted(indir.glob(args.pattern))
    if not files:
        print("対象ファイルが見つかりません:", indir / args.pattern)
        return

    total_bytes, total_chars, file_count, largest = analyze(files)
    avg_bpc = (total_bytes / total_chars) if total_chars else 0
    print(f"ファイル数: {file_count}")
    print(f"合計バイト: {total_bytes} bytes ({fmt(total_bytes)})")
    print(f"合計文字数: {total_chars}")
    print(f"平均バイト/文字: {avg_bpc:.3f}")
    print()
    print(f"上位 {args.top} ファイル (バイト順):")
    for b, chars, p in largest[:args.top]:
        bpc = (b / chars) if chars else 0
        print(f" - {p.name}: {b} bytes, {chars} chars, bytes/char={bpc:.3f}")

if __name__ == "__main__":
    main()