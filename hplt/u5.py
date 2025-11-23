#!/usr/bin/env python3
"""
hplt ディレクトリ内の .jsonl ファイルを 5 件ずつ git add/commit/push するスクリプト。
コミットメッセージは:
  (path1), (path2), ..., (path5) を追加

使用:
  cd d:\gramdata\hplt
  python u5.py

オプション:
  --dir DIR   : 対象ディレクトリ（デフォルト: カレント）
  --pattern P : ファイルパターン（デフォルト: "*.jsonl"）
  --batch N   : バッチサイズ（デフォルト: 5）
  --retries R : push リトライ回数（デフォルト: 6）
"""
from pathlib import Path
import subprocess
import argparse
import sys
import time
import random

def get_git_root(start: Path):
    try:
        p = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                           cwd=str(start), check=True, capture_output=True, text=True)
        return Path(p.stdout.strip())
    except subprocess.CalledProcessError:
        return None

def commit_and_push(files, repo_root: Path, retries=6):
    if not files:
        return True
    # make relative paths for commit message and git commands
    rel_paths = []
    for f in files:
        p = Path(f).resolve()
        try:
            rel = p.relative_to(repo_root.resolve())
            rel_paths.append(str(rel).replace("\\", "/"))
        except Exception:
            rel_paths.append(str(p))
    msg_paths = ", ".join(f"({p})" for p in rel_paths)
    msg = f"{msg_paths} を追加"
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            subprocess.run(["git", "add", "--"] + rel_paths, cwd=str(repo_root), check=True)
            # commit; if nothing to commit git returns non-zero — handle that
            cp = subprocess.run(["git", "commit", "-m", msg], cwd=str(repo_root),
                                 capture_output=True, text=True)
            if cp.returncode != 0:
                stderr = (cp.stderr or "").lower()
                stdout = (cp.stdout or "").lower()
                # nothing to commit -> treat as success/skip
                if ("nothing to commit" in stderr) or ("nothing to commit" in stdout) or ("no changes added to commit" in stderr):
                    print(f"コミット不要: {msg_paths}")
                    return True
                # other commit error -> raise to retry
                raise subprocess.CalledProcessError(cp.returncode, cp.args, output=cp.stdout, stderr=cp.stderr)
            # push
            subprocess.run(["git", "push"], cwd=str(repo_root), check=True)
            print(f"PUSH 成功: {len(rel_paths)} 件")
            return True
        except subprocess.CalledProcessError as e:
            out = (e.output or "") if hasattr(e, "output") else ""
            err = (e.stderr or "") if hasattr(e, "stderr") else ""
            print(f"git エラー (試行 {attempt}/{retries}): {str(e)}", file=sys.stderr)
            if out:
                print(out, file=sys.stderr)
            if err:
                print(err, file=sys.stderr)
            backoff = min((2 ** attempt) + random.uniform(0, 3), 300)
            print(f"{backoff:.1f}s 待機して再試行します...", file=sys.stderr)
            time.sleep(backoff)
    print("最大試行回数に達しました。手動で確認してください。", file=sys.stderr)
    return False

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", "-d", dest="dir", default=".", help="対象ディレクトリ（デフォルト: カレント）")
    p.add_argument("--pattern", dest="pattern", default="*.jsonl", help="ファイルパターン（デフォルト: *.jsonl）")
    p.add_argument("--batch", dest="batch", type=int, default=5, help="バッチサイズ（デフォルト:5）")
    p.add_argument("--retries", dest="retries", type=int, default=6, help="push リトライ回数（デフォルト:6）")
    args = p.parse_args()

    base = Path(args.dir).resolve()
    if not base.exists() or not base.is_dir():
        print("対象ディレクトリが存在しません。", file=sys.stderr)
        raise SystemExit(1)

    repo_root = get_git_root(base)
    if repo_root is None:
        print("このディレクトリは git リポジトリではありません。", file=sys.stderr)
        raise SystemExit(1)

    files = sorted([p for p in base.glob(args.pattern) if p.is_file()])
    if not files:
        print("対象ファイルが見つかりません。", file=sys.stderr)
        return

    batch = []
    for f in files:
        batch.append(f)
        if len(batch) >= args.batch:
            ok = commit_and_push(batch, repo_root, retries=args.retries)
            if not ok:
                print("バッチ処理で失敗しました。中断します。", file=sys.stderr)
                raise SystemExit(2)
            batch = []
    # leftover
    if batch:
        ok = commit_and_push(batch, repo_root, retries=args.retries)
        if not ok:
            print("最終バッチの push に失敗しました。", file=sys.stderr)
            raise SystemExit(3)

if __name__ == "__main__":
    main()