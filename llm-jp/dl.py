# d:\gramdata\llm-jp\dl.py
# -*- coding: utf-8 -*-
"""
ランダムに GitLab raw ファイル（????.jsonl.gz）をダウンロードするスクリプト。
設定はこのファイル内で行います（引数は使用しません）。

仕様:
- 各 candidate file (四桁インデックス) が選ばれる確率はデフォルト 5%（GLOBAL_PROB）。
  各エントリごとに `prob` を指定するとそれが優先されます。
- リトライ (default 3)、ストリーミングダウンロード、.part 一時ファイル→成功時にリネーム。
- 404 等は無視して次へ。
- ダウンロード先ディレクトリは各エントリごとに指定可能。
"""
from pathlib import Path
import random
import requests
import time
import os

# ---------- 設定（ここを書き換えて使ってください） ----------
# GitLab raw ベース URL（リポジトリの raw ベース）
BASE_RAW_URL = "https://gitlab.llm-jp.nii.ac.jp/datasets/llm-jp-corpus-v4/-/raw/main"

# グローバル選択確率（各ファイルが選ばれる確率）
GLOBAL_PROB = 0.05

# ランダムシード（再現性が欲しければ数値を指定、None なら非決定的）
RANDOM_SEED = None  # 例: 42

# ダウンロードのリトライ回数
RETRIES = 3
# ストリーミングチャンクサイズ
CHUNK_SIZE = 64 * 1024
# ダウンロード間の最小スリープ（軽い rate-limit 緩和）
SLEEP_BETWEEN = 1

# 各対象ディレクトリの定義リスト（必要に応じて追加・編集してください）
# remote_dir: BASE_RAW_URL の下に続くパス
# dst_dir: ローカル保存先
# index_min/index_max: 試行するインデックス範囲（inclusive）
# pattern: ファイル名のフォーマットに使います。{idx:04d} を使ってください
# prob: そのディレクトリ内の各ファイル選択確率（省略時は GLOBAL_PROB）
TARGETS = [
    {
        "name": "ja_cc_level0",
        "remote_dir": "ja/ja_cc/level0",
        "dst_dir": r"D:\gramdata\llm-jp\ja_cc\level0",
        "index_min": 0,
        "index_max": 1619,
        "pattern": "{idx:04d}.jsonl.gz",
        # "prob": 0.05,  # 指定すればこちらが優先
    },
    {
        "name": "ja_cc_level1",
        "remote_dir": "ja/ja_cc/level1",
        "dst_dir": r"D:\gramdata\llm-jp\ja_cc\level1",
        "index_min": 0,
        "index_max": 415,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "ja_cc_level2",
        "remote_dir": "ja/ja_cc/level2",
        "dst_dir": r"D:\gramdata\llm-jp\ja_cc\level2",
        "index_min": 0,
        "index_max": 404,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "ja_fineweb2",
        "remote_dir": "ja/fineweb-2",
        "dst_dir": r"D:\gramdata\llm-jp\fineweb-2",
        "index_min": 0,
        "index_max": 1670,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "ja_nwc2010",
        "remote_dir": "ja/nwc2010",
        "dst_dir": r"D:\gramdata\llm-jp\nwc2010",
        "index_min": 0,
        "index_max": 95,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "ja_patent",
        "remote_dir": "ja/patent",
        "dst_dir": r"D:\gramdata\llm-jp\patent",
        "index_min": 0,
        "index_max": 620,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "sip_comprehensive_html_20240824-20240920",
        "remote_dir": "ja/sip_comprehensive_html/20240824-20240920.toxicity_filtered",
        "dst_dir": r"D:\gramdata\llm-jp\sip_comprehensive_html\20240824-20240920.toxicity_filtered",
        "index_min": 0,
        "index_max": 49,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "sip_comprehensive_html_20240921-20240930",
        "remote_dir": "ja/sip_comprehensive_html/20240921-20240930.toxicity_filtered",
        "dst_dir": r"D:\gramdata\llm-jp\sip_comprehensive_html\20240921-20240930.toxicity_filtered",
        "index_min": 0,
        "index_max": 54,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "sip_comprehensive_html_20241001-20241019",
        "remote_dir": "ja/sip_comprehensive_html/20241001-20241019.toxicity_filtered",
        "dst_dir": r"D:\gramdata\llm-jp\sip_comprehensive_html\20241001-20241019.toxicity_filtered",
        "index_min": 0,
        "index_max": 40,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "sip_comprehensive_html_20241020-20241031",
        "remote_dir": "ja/sip_comprehensive_html/20241020-20241031.toxicity_filtered",
        "dst_dir": r"D:\gramdata\llm-jp\sip_comprehensive_html\20241020-20241031.toxicity_filtered",
        "index_min": 0,
        "index_max": 11,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "sip_comprehensive_html_20241101-20241116",
        "remote_dir": "ja/sip_comprehensive_html/20241101-20241116.toxicity_filtered",
        "dst_dir": r"D:\gramdata\llm-jp\sip_comprehensive_html\20241101-20241116.toxicity_filtered",
        "index_min": 0,
        "index_max": 22,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "sip_comprehensive_pdf",
        "remote_dir": "ja/sip_comprehensive_pdf/pdf2text",
        "dst_dir": r"D:\gramdata\llm-jp\sip_comprehensive_pdf",
        "index_min": 0,
        "index_max": 154,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "warp_pdf_e0.2",
        "remote_dir": "ja/warp_pdf/e0.2",
        "dst_dir": r"D:\gramdata\llm-jp\warp_pdf\e0.2",
        "index_min": 0,
        "index_max": 412,
        "pattern": "{idx:04d}.jsonl.gz",
    },
    {
        "name": "warp_pdf_e0",
        "remote_dir": "ja/warp_pdf/e0",
        "dst_dir": r"D:\gramdata\llm-jp\warp_pdf\e0",
        "index_min": 0,
        "index_max": 412,
        "pattern": "{idx:04d}.jsonl.gz",
    }
]
# -------------------------------------------------------------------

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def build_url(remote_dir: str, filename: str) -> str:
    # BASE_RAW_URL + "/" + remote_dir + "/" + filename
    return f"{BASE_RAW_URL.rstrip('/')}/{remote_dir.strip('/')}/{filename}"

def download_to(url: str, dst_path: Path, retries: int = RETRIES) -> bool:
    tmp = dst_path.with_suffix(dst_path.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=30) as r:
                if r.status_code == 200:
                    ensure_dir(dst_path.parent)
                    with open(tmp, "wb") as wf:
                        for chunk in r.iter_content(CHUNK_SIZE):
                            if chunk:
                                wf.write(chunk)
                    # rename to final
                    tmp.replace(dst_path)
                    return True
                elif r.status_code == 404:
                    # 存在しないファイル
                    return False
                else:
                    # 一時的な問題かもしれないのでリトライ
                    print(f"[WARN] {url} returned status {r.status_code} (attempt {attempt}/{retries})")
        except Exception as e:
            print(f"[WARN] download error {url} attempt {attempt}/{retries}: {e}")
        # wait a bit before retrying
        time.sleep(1 + attempt * 0.5)
    # cleanup tmp if exists
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass
    return False

def main():
    if RANDOM_SEED is not None:
        random.seed(RANDOM_SEED)

    total_selected = 0
    total_downloaded = 0

    for cfg in TARGETS:
        name = cfg.get("name", cfg.get("remote_dir"))
        remote_dir = cfg["remote_dir"]
        dst_dir = Path(cfg["dst_dir"])
        idx_min = int(cfg.get("index_min", 0))
        idx_max = int(cfg.get("index_max", 9999))
        pattern = cfg.get("pattern", "{idx:04d}.jsonl.gz")
        prob = float(cfg.get("prob", GLOBAL_PROB))

        print(f"[INFO] target={name} remote_dir={remote_dir} dst_dir={dst_dir} idx_range={idx_min}-{idx_max} prob={prob}")

        ensure_dir(dst_dir)
        selected_in_this = 0
        downloaded_in_this = 0

        # iterate indices
        for idx in range(idx_min, idx_max + 1):
            if random.random() > prob:
                continue  # not selected
            total_selected += 1
            selected_in_this += 1

            fname = pattern.format(idx=idx)
            url = build_url(remote_dir, fname)
            dst_path = dst_dir / fname

            # skip if already downloaded
            if dst_path.exists():
                print(f"[SKIP] already exists: {dst_path}")
                continue

            # try download
            ok = download_to(url, dst_path)
            if ok:
                print(f"[OK] downloaded: {dst_path}")
                total_downloaded += 1
                downloaded_in_this += 1
            else:
                # not found or failed
                # for 404 we silently skip; for repeated failures we already logged warnings
                print(f"[MISS] not available or failed: {url}")
            # small sleep to avoid hammering server
            time.sleep(SLEEP_BETWEEN)

        print(f"[INFO] finished target={name} selected={selected_in_this} downloaded={downloaded_in_this}")
    print(f"[SUMMARY] total_selected={total_selected} total_downloaded={total_downloaded}")

if __name__ == "__main__":
    main()