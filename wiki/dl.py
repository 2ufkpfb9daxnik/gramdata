from pathlib import Path
from datasets import load_dataset
import re

# ----- 設定（引数ではなくここに全て固定） -----
DATASET = "range3/wiki40b-ja"
SPLITS = ["train", "validation", "test"]
OUT_DIR = Path(r"D:\gramdata\wiki\data")
PREFIX = "wiki40b-ja_"
CHUNK_MB = 50  # 50 MiB ごとにファイルをローテート
PROGRESS_INTERVAL = 10000  # 何件ごとに進捗出力するか
# -----------------------------------------------

def find_next_index(out_dir: Path, prefix: str):
    max_idx = 0
    pattern = re.compile(re.escape(prefix) + r"(\d{4})\.txt$")
    if not out_dir.exists():
        return 1
    for p in out_dir.glob(f"{prefix}*.txt"):
        m = pattern.search(p.name)
        if m:
            try:
                i = int(m.group(1))
                if i > max_idx:
                    max_idx = i
            except:
                pass
    return max_idx + 1

def sanitize_line(text: str) -> str:
    if text is None:
        return ""
    # 改行類をスペースに置換して1行にまとめる
    return text.replace('\r', ' ').replace('\n', ' ').strip()

def ensure_out_dir(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

def download_all_dataset(dataset_name: str):
    # 全 split を一括でダウンロード（キャッシュに落とす）
    print(f"[INFO] downloading dataset {dataset_name} (all splits) ... this may take long and use much disk")
    _ = load_dataset(dataset_name)
    print("[INFO] dataset download complete (cached)")

def write_texts(dataset_name: str, splits, out_dir: Path, prefix: str, chunk_mb: int):
    chunk_bytes = chunk_mb * 1024 * 1024
    ensure_out_dir(out_dir)

    current_index = find_next_index(out_dir, prefix)
    current_file = None
    current_written = 0  # bytes written in current file
    total_written = 0
    total_items = 0

    def open_new_file(idx):
        nonlocal current_file, current_written
        if current_file:
            current_file.close()
        fname = out_dir / f"{prefix}{idx:04d}.txt"
        current_file = open(fname, "wb")
        current_written = 0
        print(f"[INFO] opened {fname}")
        return fname

    # 初期ファイルを開く
    open_new_file(current_index)

    try:
        for split in splits:
            print(f"[INFO] processing split: {split} (loading into local cache if needed)")
            ds = load_dataset(dataset_name, split=split)  # 全データをローカルに読み込み／キャッシュ（Arrow）
            print(f"[INFO] split {split} has {len(ds)} rows")
            count = 0
            for ex in ds:
                count += 1
                total_items += 1

                text = ex.get("text", "")
                line = sanitize_line(text)
                if not line:
                    continue
                encoded = line.encode("utf-8")
                line_bytes = encoded + b"\n"
                if current_written + len(line_bytes) > chunk_bytes:
                    # rotate
                    current_file.close()
                    current_index += 1
                    open_new_file(current_index)
                current_file.write(line_bytes)
                current_written += len(line_bytes)
                total_written += len(line_bytes)

                if total_items % PROGRESS_INTERVAL == 0:
                    print(f"[PROGRESS] items={total_items:,}, current_file_bytes={current_written:,}, total_written={total_written:,}")
            print(f"[INFO] finished split {split}: iterated {count} items")
    finally:
        if current_file:
            current_file.close()

    print(f"[DONE] total items processed: {total_items:,}, total bytes written: {total_written:,}")
    print(f"[DONE] last index: {current_index:04d}")

def main():
    print("[CONFIG] fixed configuration:")
    print(f"  DATASET = {DATASET}")
    print(f"  SPLITS  = {SPLITS}")
    print(f"  OUT_DIR = {OUT_DIR}")
    print(f"  PREFIX  = {PREFIX}")
    print(f"  CHUNK_MB = {CHUNK_MB}")
    print()

    # 1) まず全データをダウンロード（キャッシュ）しておく
    download_all_dataset(DATASET)

    # 2) text 要素を各 split ごとに書き出す（50MiB ごとにローテート）
    write_texts(DATASET, SPLITS, OUT_DIR, PREFIX, CHUNK_MB)

if __name__ == "__main__":
    main()
