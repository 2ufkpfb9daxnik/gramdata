"""
nwc2010/nwc2010-ngrams 以下の .xz ファイルを解凍し、
テキストをほぼ 50MB ごとに分割して
元のパス構造を保ったまま .txt を出力するスクリプト。

さらに、分割したファイルを 5 件ごとに git add/commit/push します。
push が失敗したらランダムな待ち時間でリトライします。

例:
  入力: nwc2010-ngrams/word/over99/2gms/2gm-0001.xz
  出力: nwc2010/nwc2010-ngrams/word/over99/2gms/2gm-0000.txt, 2gm-0001.txt, ...
"""
from pathlib import Path
import argparse
import lzma
import sys
import re
import subprocess
import time
import random
from subprocess import CalledProcessError

# グローバル: コミット待ちファイルリストとリポジトリルート
_pending_files = []
_repo_root = None
_BATCH_SIZE = 5

def find_xz_files(root: Path):
    return sorted(root.rglob("*.xz"))

def ensure_dir_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def make_out_paths(xz_path: Path, in_root: Path, out_root: Path):
    # rel = path relative to in_root, keep parent dirs
    rel = xz_path.relative_to(in_root)
    out_dir = out_root / rel.parent
    # original stem (without .xz)
    stem = xz_path.stem
    # try to find trailing 4 digits in stem
    m = re.search(r'(\d{4})$', stem)
    if m:
        prefix = stem[:-4]  # keep trailing separator (e.g. "2gm-")
    else:
        prefix = stem + "_"  # will produce e.g. name_0000.txt
    return out_dir, prefix

def get_git_root(start_path: Path):
    try:
        p = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                           cwd=str(start_path), check=True, capture_output=True, text=True)
        return Path(p.stdout.strip())
    except CalledProcessError:
        return None

def commit_and_push(files, repo_root: Path, retries=6):
    if not files:
        return True
    # convert to paths relative to repo root when possible
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
            # git add
            r = subprocess.run(["git", "add", "--"] + rel_paths, cwd=str(repo_root),
                               check=True, capture_output=True, text=True)
            # check if anything is staged
            diff = subprocess.run(["git", "diff", "--cached", "--name-only"], cwd=str(repo_root),
                                  check=True, capture_output=True, text=True)
            if not diff.stdout.strip():
                print("コミットする差分がありません、スキップします。")
                return True
            # git commit
            subprocess.run(["git", "commit", "-m", msg], cwd=str(repo_root), check=True,
                           capture_output=True, text=True)
            # git push
            subprocess.run(["git", "push"], cwd=str(repo_root), check=True,
                           capture_output=True, text=True)
            print(f"git push 成功: {len(files)} 件 -> {msg}")
            return True
        except CalledProcessError as e:
            stderr = (e.stderr or "").strip()
            stdout = (e.stdout or "").strip()
            print(f"git 操作失敗 (試行 {attempt}/{retries}): {e}. stdout={stdout} stderr={stderr}", file=sys.stderr)
            backoff = (2 ** attempt) + random.uniform(0, 3)
            sleep_time = min(backoff, 300)
            print(f"{sleep_time:.1f}s 待機して再試行します...", file=sys.stderr)
            time.sleep(sleep_time)
    print("git push が最大試行回数で失敗しました。手動で確認してください。", file=sys.stderr)
    return False

def _flush_pending_if_needed():
    global _pending_files, _repo_root
    if _repo_root is None:
        return
    if len(_pending_files) >= _BATCH_SIZE:
        to_commit = _pending_files[:]
        _pending_files = []
        commit_and_push(to_commit, _repo_root)

def unpack_and_split(xz_path: Path, in_root: Path, out_root: Path, max_bytes: int, force: bool=False):
    global _pending_files, _repo_root
    out_dir, prefix = make_out_paths(xz_path, in_root, out_root)
    try:
        src = lzma.open(xz_path, mode="rt", encoding="utf-8", errors="replace")
    except Exception as e:
        print(f"解凍失敗: {xz_path} -> {e}", file=sys.stderr)
        return False

    ensure_dir_dir(out_dir)

    idx = 0
    out_fp = None
    bytes_written = 0
    written_any = False
    last_out_path = None
    try:
        for line in src:
            b = line.encode("utf-8")
            if out_fp is None:
                out_name = f"{prefix}{idx:04d}.txt"
                out_path = out_dir / out_name
                if out_path.exists() and not force:
                    # 上書きしない方針でもここでは上書きする設計
                    pass
                out_fp = open(out_path, "wb")
                bytes_written = 0
                last_out_path = out_path
            if bytes_written + len(b) > max_bytes and bytes_written > 0:
                out_fp.close()
                # 生成済みファイルをコミット待ちに追加
                if last_out_path is not None:
                    _pending_files.append(str(last_out_path))
                    _flush_pending_if_needed()
                idx += 1
                out_name = f"{prefix}{idx:04d}.txt"
                out_path = out_dir / out_name
                out_fp = open(out_path, "wb")
                bytes_written = 0
                last_out_path = out_path
            out_fp.write(b)
            bytes_written += len(b)
            written_any = True
    finally:
        if out_fp is not None:
            out_fp.close()
            if last_out_path is not None:
                _pending_files.append(str(last_out_path))
                _flush_pending_if_needed()
        src.close()

    print(f"完了: {xz_path} -> {out_dir} (分割ファイル数: {idx+1 if written_any else 0})")
    return True

def main():
    global _repo_root, _pending_files
    parser = argparse.ArgumentParser(description="nwc2010 の .xz を解凍して約 指定MB ごとに分割する（出力は入力と同じパス構造）")
    parser.add_argument("--in", dest="in_root", default="nwc2010-ngrams",
                        help="入力ルートディレクトリ (デフォルト: nwc2010-ngrams)")
    parser.add_argument("--out", dest="out_root", default=None,
                        help="出力ルートディレクトリ（デフォルト: in_root の親、つまり nwc2010）")
    parser.add_argument("--size-mb", dest="size_mb", type=int, default=50,
                        help="1ファイルあたりの目標サイズ (MB, デフォルト:50)")
    parser.add_argument("--force", action="store_true", help="既存出力を上書きする")
    args = parser.parse_args()

    in_root = Path(args.in_root).resolve()
    if args.out_root:
        out_root = Path(args.out_root).resolve()
    else:
        out_root = in_root.parent.resolve()  # 入力ルートの親に出力（通常は nwc2010）
    max_bytes = args.size_mb * 1024 * 1024

    if not in_root.exists():
        print(f"入力ディレクトリが見つかりません: {in_root}", file=sys.stderr)
        sys.exit(1)

    # Git リポジトリルートを取得（見つからなければ自動コミット機能は無効）
    _repo_root = get_git_root(out_root)
    if _repo_root is None:
        print("警告: 出力先は git リポジトリ外です。自動コミット/プッシュは無効になります。", file=sys.stderr)

    for xz in find_xz_files(in_root):
        try:
            unpack_and_split(xz, in_root, out_root, max_bytes, force=args.force)
        except Exception as e:
            print(f"処理中に例外: {xz} -> {e}", file=sys.stderr)

    # 残っているファイルをコミット
    if _repo_root is not None and _pending_files:
        to_commit = _pending_files[:]
        _pending_files = []
        commit_and_push(to_commit, _repo_root)

if __name__ == "__main__":
    main()
