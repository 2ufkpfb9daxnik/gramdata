# ...existing code...
#!/usr/bin/env python3
"""
シンプルなダウンローダー:
  python dl.py filelist100morpheme
wget -x -nH -i filelist と同様にホスト名を除いたパス構造を再現して保存します。
"""
import argparse
import os
import sys
import time
from urllib.parse import urlparse
import requests

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def download_url(url, dest_path, session, retries=3, chunk_size=1024*64):
    tmp_path = dest_path + ".part"
    for attempt in range(1, retries+1):
        try:
            # HEAD でサイズを取る（存在チェック用）
            head = session.head(url, allow_redirects=True, timeout=10)
            if head.status_code >= 400:
                # fallback to GET if HEAD not allowed
                resp = session.get(url, stream=True, timeout=30)
            else:
                # If file exists and sizes match, skip
                remote_len = head.headers.get("Content-Length")
                if os.path.exists(dest_path) and remote_len is not None:
                    if os.path.getsize(dest_path) == int(remote_len):
                        print(f"既存のためスキップ: {dest_path}")
                        return True
                resp = session.get(url, stream=True, timeout=30)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}")
            ensure_dir(os.path.dirname(dest_path))
            with open(tmp_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if chunk:
                        fh.write(chunk)
            os.replace(tmp_path, dest_path)
            print(f"ダウンロード完了: {dest_path}")
            return True
        except Exception as e:
            print(f"[{attempt}/{retries}] エラー: {url} -> {e}", file=sys.stderr)
            time.sleep(1 + attempt)
    # 最終的に失敗
    if os.path.exists(tmp_path):
        try:
            os.remove(tmp_path)
        except Exception:
            pass
    return False

def process_filelist(filelist_path, out_root):
    session = requests.Session()
    session.headers.update({"User-Agent": "python-dl/1.0"})
    success = 0
    fail = 0
    with open(filelist_path, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            p = urlparse(url)
            if not p.scheme or not p.netloc:
                print(f"無効なURLをスキップ: {url}", file=sys.stderr)
                fail += 1
                continue
            # wget -nH の挙動: ホスト名ディレクトリを作らず、パス部分をそのまま再現
            rel_path = p.path.lstrip("/")  # leading slash を削る
            dest_path = os.path.join(out_root, rel_path)
            ok = download_url(url, dest_path, session)
            if ok:
                success += 1
            else:
                fail += 1
    print(f"完了: 成功={success} 失敗={fail}")

def main():
    parser = argparse.ArgumentParser(description="filelist から wget -x -nH 相当でダウンロードする")
    parser.add_argument("filelist", nargs="?", default="filelist100word",
                        help="URLリストファイル（デフォルト: filelist100word）")
    parser.add_argument("-o", "--out", default=".", help="出力ルートディレクトリ（デフォルト: カレント）")
    args = parser.parse_args()
    if not os.path.exists(args.filelist):
        print(f"filelist が見つかりません: {args.filelist}", file=sys.stderr)
        sys.exit(1)
    process_filelist(args.filelist, args.out)

if __name__ == "__main__":
    main()
