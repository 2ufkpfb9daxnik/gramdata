#!/usr/bin/env python3
import os
import json
import re
import sys
import tempfile
import importlib
from pathlib import Path
from collections import Counter

# --- 設定（必要なら編集） ---
SUDACHI_FULL_RES = r'D:\gramdata\.venv\Lib\site-packages\sudachidict_full\resources'
IN_DIR = Path("data")
PATTERN = "10_*.jsonl"
OUT_DIR = Path("word")
MIN_COUNT = 10    # 出現回数がこの値未満の n-gram は出力しない
NGRAM_MAX = 7
# ------------------------------

# 強制的に full 辞書を使わせる（import 前に必ず設定）
os.environ["SUDACHIPY_DICT"] = SUDACHI_FULL_RES

# import
try:
    from sudachipy import dictionary, tokenizer
    from sudachipy.errors import SudachiError
except Exception as e:
    print("SudachiPy の import に失敗しました。SUDACHI_FULL_RES を確認してください。", file=sys.stderr)
    print("詳細:", e, file=sys.stderr)
    raise SystemExit(1)

# 日本語トークン判定（ひらがな/カタカナ/漢字 と CJK 句読点・全角記号・ダッシュを含む）
_JP_RE = re.compile(
    r'['
    r'\u3040-\u309F'  # ひらがな
    r'\u30A0-\u30FF'  # カタカナ（ー含む）
    r'\u4E00-\u9FFF'  # 漢字
    r'\u3000-\u303F'  # CJK symbols（。、等）
    r'\uFF00-\uFFEF'  # 全角記号（？、！等）
    r'\u2010-\u2015'  # ダッシュ類
    r']'
)

def is_japanese_token(s: str) -> bool:
    return bool(_JP_RE.search(s))

def jsonl_iter_texts(path: Path):
    txt_re = re.compile(r'"text"\s*:\s*"')
    for raw in path.open("r", encoding="utf-8", errors="replace"):
        line = raw.rstrip("\n")
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                t = obj.get("text") or obj.get("body") or obj.get("content")
                if t:
                    yield t
                    continue
        except Exception:
            if txt_re.search(line):
                m2 = re.search(r'"text"\s*:\s*"((?:\\.|[^"\\])*)"', line)
                if m2:
                    rawtxt = m2.group(1)
                    try:
                        t = bytes(rawtxt, "utf-8").decode("unicode_escape")
                        yield t
                        continue
                    except Exception:
                        pass
            continue

def _create_sudachi_tokenizer(res_path: Path):
    """
    辞書初期化を多段的に試みる。通常の自動検出で失敗したら、
    sudachi.json をテンポラリに作成して absolute system.dic を参照させて試す。
    戻り値は Tokenizer インスタンス。
    """
    # 1) 通常の自動検出
    try:
        return dictionary.Dictionary().create()
    except ModuleNotFoundError:
        pass
    except SudachiError:
        pass
    except Exception:
        pass

    # 2) sudachi.json と system.dic があるならテンポラリの設定ファイルを作って試す
    sudachi_json = res_path / "sudachi.json"
    system_dic = res_path / "system.dic"
    if sudachi_json.exists() and system_dic.exists():
        try:
            tpl = json.loads(sudachi_json.read_text(encoding="utf-8"))
        except Exception:
            tpl = {}
        tpl["systemDict"] = str(system_dic.resolve())
        if "characterDefinitionFile" not in tpl:
            tpl["characterDefinitionFile"] = "char.def"
        tf = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
        try:
            json.dump(tpl, tf, ensure_ascii=False, indent=2)
            tf.flush()
            tf.close()
            try:
                return dictionary.Dictionary(str(tf.name)).create()
            except Exception:
                importlib.reload(dictionary)
                return dictionary.Dictionary(str(tf.name)).create()
        finally:
            try:
                os.unlink(tf.name)
            except Exception:
                pass

    raise RuntimeError("Sudachi dictionary の初期化に失敗しました。sudachidict_full の resources を確認してください。")

def write_counter(counter: Counter, out_dir: Path, n: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{n}hplt0000.txt"
    with out_path.open("w", encoding="utf-8") as f:
        for gram, cnt in counter.most_common():
            if cnt >= MIN_COUNT:
                f.write(f"{gram}\t{cnt}\n")
    return out_path

def main():
    res_path = Path(SUDACHI_FULL_RES)
    if not res_path.is_dir() or not (res_path / "system.dic").exists():
        print(f"辞書 resources が見つからないか system.dic がありません: {res_path}", file=sys.stderr)
        raise SystemExit(1)

    if not IN_DIR.exists():
        print("入力ディレクトリが存在しません:", IN_DIR, file=sys.stderr)
        raise SystemExit(1)

    files = sorted(IN_DIR.glob(PATTERN))
    if not files:
        print("処理対象ファイルが見つかりません。", file=sys.stderr)
        return

    try:
        tok = _create_sudachi_tokenizer(res_path)
    except Exception as e:
        print("辞書の初期化に失敗しました。", file=sys.stderr)
        print("詳細:", e, file=sys.stderr)
        raise SystemExit(1)

    split_mode = tokenizer.Tokenizer.SplitMode.B

    counters = {n: Counter() for n in range(1, NGRAM_MAX + 1)}

    for src in files:
        print("processing", src.name)
        for text in jsonl_iter_texts(src):
            ms = []
            try:
                ms = tok.tokenize(text, split_mode)
            except Exception:
                ms = []
            surfaces = [m.surface() for m in ms if is_japanese_token(m.surface())]
            if not surfaces:
                continue
            L = len(surfaces)
            for n in range(1, min(NGRAM_MAX, L) + 1):
                c = counters[n]
                for i in range(0, L - n + 1):
                    gram = " ".join(surfaces[i:i+n])
                    c[gram] += 1

    written = []
    for n in range(1, NGRAM_MAX + 1):
        outp = write_counter(counters[n], OUT_DIR, n)
        written.append(outp)
        cnt_items = sum(1 for _, v in counters[n].items() if v >= MIN_COUNT)
        print(f"written {outp} ({cnt_items} items >= {MIN_COUNT})")

    print("完了。出力ディレクトリ:", OUT_DIR)

if __name__ == "__main__":
    main()