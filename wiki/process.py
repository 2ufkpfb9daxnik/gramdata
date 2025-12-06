"""
Process wiki text files in-place.

Behavior:
- Remove tokens: _START_PARAGRAPH_, _NEWLINE_.
- When encountering _START_ARTICLE_ or _START_SECTION_, remove that token
  and drop all following content up to (but not including) the next _START_PARAGRAPH_.
  The following _START_PARAGRAPH_ is also removed.
- Process each file streamingly and write to a temporary file, then atomically
  replace the original. No backup of the original will be kept (MAKE_BACKUP=False).
"""
from pathlib import Path
import re
import shutil

# ----- 固定設定（ここで変更） -----
IN_DIR = Path(r"D:\gramdata\wiki\data")
PREFIX = "wiki40b-ja_"
GLOB_PATTERN = f"{PREFIX}*.txt"
ENCODING = "utf-8"
MAKE_BACKUP = False  # バックアップを取らない
BACKUP_DIR = IN_DIR / "backup_originals"
# -------------------------------

TOK_START_PARAGRAPH = "_START_PARAGRAPH_"
TOK_START_ARTICLE = "_START_ARTICLE_"
TOK_START_SECTION = "_START_SECTION_"
TOK_NEWLINE = "_NEWLINE_"

ALL_TOKENS = [TOK_START_ARTICLE, TOK_START_SECTION, TOK_START_PARAGRAPH, TOK_NEWLINE]
TOKEN_REGEX = re.compile("(" + "|".join(re.escape(t) for t in ALL_TOKENS) + ")")

def process_line_stream(lines):
    """
    Generator: process input lines and yield output lines (no trailing newline).
    skip_to_paragraph state is per-file (not carried across files).
    """
    skip_to_paragraph = False

    for line in lines:
        s = line.rstrip("\n")
        i = 0
        out_chunks = []
        L = len(s)

        while i < L:
            if not skip_to_paragraph:
                m = TOKEN_REGEX.search(s, i)
                if not m:
                    out_chunks.append(s[i:])
                    i = L
                else:
                    j = m.start()
                    tok = m.group(1)
                    if j > i:
                        out_chunks.append(s[i:j])
                    if tok == TOK_NEWLINE or tok == TOK_START_PARAGRAPH:
                        # remove token, continue scanning after it
                        i = m.end()
                    elif tok == TOK_START_ARTICLE or tok == TOK_START_SECTION:
                        # enter skip mode until next START_PARAGRAPH
                        skip_to_paragraph = True
                        i = m.end()
                    else:
                        i = m.end()
            else:
                # skipping until next START_PARAGRAPH
                m = TOKEN_REGEX.search(s, i)
                if not m:
                    # skip remainder of this line
                    i = L
                else:
                    tok = m.group(1)
                    if tok == TOK_START_PARAGRAPH:
                        # consume it and exit skip mode (do not emit)
                        skip_to_paragraph = False
                        i = m.end()
                    else:
                        # consume and keep skipping
                        i = m.end()
        out_line = "".join(out_chunks).strip()
        if out_line:
            yield out_line

def process_file_inplace(in_path: Path, make_backup: bool = True, backup_dir: Path = None):
    if not in_path.is_file():
        return
    tmp_path = in_path.with_suffix(in_path.suffix + ".tmp")
    if make_backup and backup_dir is None:
        backup_dir = in_path.parent / "backup_originals"
    if make_backup:
        backup_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] processing {in_path.name} -> (tmp) {tmp_path.name}")
    # Stream read/write
    try:
        with in_path.open("r", encoding=ENCODING, errors="replace") as inf, \
             tmp_path.open("w", encoding=ENCODING, newline="\n") as outf:
            for out_line in process_line_stream(inf):
                outf.write(out_line + "\n")
    except Exception:
        # If something failed, ensure tmp is removed
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
        raise

    # Replace original safely: if make_backup is False, perform direct replace
    if make_backup:
        backup_path = backup_dir / in_path.name
        # If backup already exists, append numeric suffix
        if backup_path.exists():
            k = 1
            while True:
                candidate = backup_dir / f"{in_path.stem}.orig.{k}{in_path.suffix}"
                if not candidate.exists():
                    backup_path = candidate
                    break
                k += 1
        shutil.move(str(in_path), str(backup_path))
        tmp_path.replace(in_path)
        print(f"[INFO] original backed up to {backup_path.name}, replaced with processed file")
    else:
        # Direct replace (atomic if same FS)
        tmp_path.replace(in_path)
        print(f"[INFO] replaced original with processed file (no backup)")

def main():
    if not IN_DIR.exists():
        print(f"[ERROR] input directory not found: {IN_DIR}")
        return
    files = sorted(IN_DIR.glob(GLOB_PATTERN))
    if not files:
        print(f"[WARN] no files matching {GLOB_PATTERN} in {IN_DIR}")
        return

    for p in files:
        try:
            process_file_inplace(p, make_backup=MAKE_BACKUP, backup_dir=BACKUP_DIR if MAKE_BACKUP else None)
        except Exception as e:
            print(f"[ERROR] failed processing {p.name}: {e}")

if __name__ == "__main__":
    main()