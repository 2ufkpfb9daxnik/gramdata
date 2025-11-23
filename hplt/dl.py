#!/usr/bin/env python3
"""
HPLT の指定ファイル (デフォルト: jpn_Jpan/10_1.jsonl.zst) の
- リモートサイズ確認 (HEAD)
- ダウンロード（途中からの再試行付き）
- md5 検証（jpn_Jpan.md5 を取得して照合）

使い方例:
  python download_jpn10.py --check-size
  python download_jpn10.py --download --verify
"""
from pathlib import Path
import argparse
import requests
from urllib.parse import urlparse
import os
import sys
import time
import random
import hashlib

DEFAULT_URL = "https://data.hplt-project.org/three/sorted/jpn_Jpan/10_1.jsonl.zst"
DEFAULT_MD5 = "https://data.hplt-project.org/three/sorted/jpn_Jpan.md5"

def get_remote_size(url, timeout=15):
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        if r.status_code >= 400:
            return None, f"HEAD returned {r.status_code}"
        cl = r.headers.get("Content-Length")
        if cl:
            return int(cl), None
        # Content-Length 無ければ None を返す（必要なら GET で確認するが慎重に）
        return None, None
    except Exception as e:
        return None, str(e)

def fetch_md5_map(md5_url, timeout=30):
    """md5 ファイルを取得して {filename: md5} の dict を返す。
       md5 ファイルの形式は一般的に: "<md5>  <path>" の行を想定。
    """
    try:
        r = requests.get(md5_url, timeout=timeout)
        r.raise_for_status()
        mapping = {}
        for line in r.text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                chksum = parts[0]
                fname = parts[-1]
                # store both basename and full path keys for検索
                mapping[fname] = chksum
                mapping[Path(fname).name] = chksum
        return mapping, None
    except Exception as e:
        return {}, str(e)

def md5_of(path, chunk=8*1024*1024):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(chunk), b""):
            h.update(b)
    return h.hexdigest()

def download_with_retries(url, dest_path: Path, retries=6, timeout=60, chunk_size=1024*1024):
    tmp = dest_path.with_suffix(dest_path.suffix + ".part")
    attempt = 0
    last_report = 0
    while attempt < retries:
        attempt += 1
        try:
            # range resume if partial exists
            headers = {}
            mode = "wb"
            existing = tmp.exists() and tmp.stat().st_size or 0
            if existing:
                headers["Range"] = f"bytes={existing}-"
                mode = "ab"
            downloaded = existing
            start = time.time()
            with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
                r.raise_for_status()
                total_len = r.headers.get("Content-Length")
                try:
                    total_len = int(total_len) + existing if total_len is not None else None
                except Exception:
                    total_len = None
                with open(tmp, mode) as fh:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            fh.write(chunk)
                            downloaded += len(chunk)
                            now = time.time()
                            if now - last_report > 3:  # 3秒ごとに報告
                                last_report = now
                                if total_len:
                                    pct = downloaded * 100 / total_len
                                    print(f"ダウンロード: {downloaded:,} / {total_len:,} bytes ({pct:.1f}%)")
                                else:
                                    print(f"ダウンロード: {downloaded:,} bytes")
            tmp.replace(dest_path)
            return True, None
        except Exception as e:
            wait = min((2 ** attempt) + random.random()*3, 300)
            print(f"ダウンロード失敗: 試行 {attempt}/{retries} -> {e}. {wait:.1f}s 後に再試行します...", file=sys.stderr)
            time.sleep(wait)
    return False, f"ダウンロードに失敗しました（{retries} 回）"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL, help="ダウンロード対象の URL")
    p.add_argument("--md5", default=DEFAULT_MD5, help="言語 md5 ファイルの URL")
    p.add_argument("--out", default=".", help="出力ルート（既定: カレント）")
    p.add_argument("--check-size", action="store_true", help="リモートの Content-Length を確認して表示するだけ")
    p.add_argument("--download", action="store_true", help="ダウンロードを実行する")
    p.add_argument("--verify", action="store_true", help="ダウンロード後に md5 検証を行う")
    args = p.parse_args()

    url = args.url
    md5_url = args.md5
    out_root = Path(args.out)

    # リモートサイズ確認
    size, err = get_remote_size(url)
    if err:
        print(f"HEAD エラー: {err}", file=sys.stderr)
    if size is None:
        print("リモート Content-Length: 不明")
    else:
        print(f"リモート Content-Length: {size} bytes ({size/1024/1024:.2f} MB)")

    if args.check_size and not args.download:
        return

    # 出力パスを URL のパス構造に基づいて作る（例: three/sorted/jpn_Jpan/10_1.jsonl.zst -> out_root/three/sorted/jpn_Jpan/10_1.jsonl.zst）
    up = urlparse(url)
    rel_path = Path(up.path.lstrip("/"))
    dest_path = (out_root / rel_path).resolve()
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    if args.download:
        ok, err = download_with_retries(url, dest_path)
        if not ok:
            print(f"ダウンロード失敗: {err}", file=sys.stderr)
            sys.exit(1)
        print(f"保存: {dest_path} ({dest_path.stat().st_size} bytes)")

        if args.verify:
            md5map, err = fetch_md5_map(md5_url)
            if err:
                print(f"md5 リスト取得失敗: {err}", file=sys.stderr)
                sys.exit(2)
            # md5map に対してキーを探す（まずフルパス相対、次に basename）
            key1 = str(rel_path)
            key2 = rel_path.name
            chksum = md5map.get(key1) or md5map.get(key2)
            if not chksum:
                print("md5 エントリが見つかりません（md5 マップにファイル名がない）", file=sys.stderr)
                print("取得した md5map のキー例:", list(md5map.keys())[:10])
                sys.exit(3)
            actual = md5_of(dest_path)
            if actual.lower() == chksum.lower():
                print("md5 OK")
            else:
                print(f"md5 mismatch: expected {chksum} actual {actual}", file=sys.stderr)
                sys.exit(4)

if __name__ == "__main__":
    main()