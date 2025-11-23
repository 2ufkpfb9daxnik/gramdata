import os
import json
import re
import glob
from collections import defaultdict

# --- 設定 ---
INPUT_DIR = r"d:\gramdata\pr"  # 入力JSONファイルがあるディレクトリ
KANA_FILE = os.path.join(r"d:\gramdata", "kana.txt")
OUTPUT_BASE_DIR = os.path.join(r"d:\gramdata", "pr_processed")

# --- ローマ字変換マップ ---
basic_roma_map = {
    'あ': 'a', 'い': 'i', 'う': 'u', 'え': 'e', 'お': 'o',
    'か': 'ka', 'き': 'ki', 'く': 'ku', 'け': 'ke', 'こ': 'ko',
    'さ': 'sa', 'し': 'shi', 'す': 'su', 'せ': 'se', 'そ': 'so',
    'た': 'ta', 'ち': 'chi', 'つ': 'tsu', 'て': 'te', 'と': 'to',
    'な': 'na', 'に': 'ni', 'ぬ': 'nu', 'ね': 'ne', 'の': 'no',
    'は': 'ha', 'ひ': 'hi', 'ふ': 'fu', 'へ': 'he', 'ほ': 'ho',
    'ま': 'ma', 'み': 'mi', 'む': 'mu', 'め': 'me', 'も': 'mo',
    'や': 'ya', 'ゆ': 'yu', 'よ': 'yo',
    'ら': 'ra', 'り': 'ri', 'る': 'ru', 'れ': 're', 'ろ': 'ro',
    'わ': 'wa', 'を': 'wo',
    'ん': 'n',
    'が': 'ga', 'ぎ': 'gi', 'ぐ': 'gu', 'げ': 'ge', 'ご': 'go',
    'ざ': 'za', 'じ': 'ji', 'ず': 'zu', 'ぜ': 'ze', 'ぞ': 'zo',
    'だ': 'da', 'ぢ': 'di', 'づ': 'zu', 'で': 'de', 'ど': 'do',
    'ば': 'ba', 'び': 'bi', 'ぶ': 'bu', 'べ': 'be', 'ぼ': 'bo',
    'ぱ': 'pa', 'ぴ': 'pi', 'ぷ': 'pu', 'ぺ': 'pe', 'ぽ': 'po',
    'ゐ': 'wi', 'ゑ': 'we',
}
# プレフィックスとして扱わない文字（拗音・促音など）
ignore_chars_for_prefix = set('っゃゅょゎぁぃぅぇぉ')

# --- JSONファイルのパターン定義 ---
file_patterns = {
    'emojinarabeasobi': r'emojinarabeasobi(\d+)gram\.json',
    'singeta': r'singeta(\d+)gram\.json',
    'tsukimiso': r'tsukimiso(\d+)gram\.json',
    'wikikana': r'wikikana(\d+)gram\.json',
    'dvorakjpen': r'dvorakjpen\.json',
    'dvorakjpkana': r'dvorakjpkana\.json',
    'dvorakjproman': r'dvorakjproman\.json'
}

def get_file_category(filename):
    """ファイル名からカテゴリと文字列長を判定する"""
    for category, pattern in file_patterns.items():
        match = re.match(pattern, os.path.basename(filename))
        if match:
            # 文字数を取得（dvorakjp系は含まれない）
            if len(match.groups()) > 0:
                n_gram = int(match.group(1))
                return category, n_gram
            else:
                return category, 1  # dvorakjpシリーズは1文字として扱う
    
    # dvorakjpの特別扱い
    if os.path.basename(filename).startswith("dvorakjp"):
        if "en" in filename:
            return "dvorakjpen", 1
        elif "kana" in filename:
            return "dvorakjpkana", 1
        elif "roman" in filename:
            return "dvorakjproman", 1
    
    return None, None

# --- kana.txt から対象ひらがなを取得 ---
target_hiragana_1char = set()
try:
    with open(KANA_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            # コメント行や空行をスキップ
            if not line or line.startswith('//'):
                continue
            for char in line:
                # プレフィックスとして扱わない文字は除外
                if char not in ignore_chars_for_prefix:
                    target_hiragana_1char.add(char)
except FileNotFoundError:
    print(f"Error: Kana definition file not found at {KANA_FILE}.")
    exit()
except Exception as e:
    print(f"Error reading {KANA_FILE}: {e}")
    exit()

# --- 1文字プレフィックス用のローマ字マップを作成 ---
roma_map_1char = {}
for hira in target_hiragana_1char:
    if hira in basic_roma_map:
        roma_map_1char[hira] = basic_roma_map[hira]
    else:
        # basic_roma_map に定義がない文字は警告を出してスキップ
        print(f"Warning: No basic roma mapping found for '{hira}'. Skipping this character for prefix.")

def process_json_file(file_path):
    """JSONファイルを読み込み、変換処理を行う"""
    category, n_gram_length = get_file_category(file_path)
    
    if not category or not n_gram_length:
        print(f"Skipping unknown format file: {file_path}")
        return None, None

    print(f"Processing {file_path} (Category: {category}, N-gram: {n_gram_length})")

    # 出力データの構造を初期化
    output_data = defaultdict(lambda: defaultdict(dict))
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # データを処理
            for text, freq in data.items():
                # 文字列の長さを取得
                current_n_gram_length = len(text)
                if current_n_gram_length == 0:
                    continue  # 空の文字列はスキップ

                # --- 全ての1文字プレフィックスを処理 ---
                for i in range(current_n_gram_length):
                    char = text[i]
                    if char in roma_map_1char:
                        prefix1_roma = roma_map_1char[char]
                        output_data[prefix1_roma][current_n_gram_length][text] = freq

                # --- 全ての連続2文字プレフィックスを処理 ---
                for i in range(current_n_gram_length - 1):
                    char1 = text[i]
                    char2 = text[i+1]
                    if char1 in roma_map_1char and char2 in roma_map_1char:
                        prefix2_roma = roma_map_1char[char1] + roma_map_1char[char2]
                        output_data[prefix2_roma][current_n_gram_length][text] = freq
    
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {file_path}")
        return None, None
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None, None
    
    return category, output_data

def main():
    # 出力ベースディレクトリを作成
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)
    
    # カテゴリごとのデータを格納する辞書
    category_data = {}
    
    # prディレクトリ内のすべてのJSONファイルを処理
    json_files = glob.glob(os.path.join(INPUT_DIR, "*.json"))
    
    if not json_files:
        print(f"No JSON files found in {INPUT_DIR}")
        return
    
    for json_file in json_files:
        category, data = process_json_file(json_file)
        
        if category and data:
            if category not in category_data:
                category_data[category] = defaultdict(lambda: defaultdict(dict))
            
            # カテゴリごとにデータを蓄積（重複する場合は後のファイルの値で上書き）
            for prefix, n_gram_data in data.items():
                for n_gram_len, text_data in n_gram_data.items():
                    for text, freq in text_data.items():
                        category_data[category][prefix][n_gram_len][text] = freq
    
    # カテゴリごとに出力処理
    for category, prefix_data in category_data.items():
        # カテゴリのディレクトリを作成
        category_dir = os.path.join(OUTPUT_BASE_DIR, category)
        os.makedirs(category_dir, exist_ok=True)
        
        print(f"Creating files for category: {category}")
        
        # プレフィックスごとのディレクトリとファイルを作成
        for prefix, n_gram_data in prefix_data.items():
            # プレフィックスディレクトリを作成
            prefix_dir = os.path.join(category_dir, prefix)
            os.makedirs(prefix_dir, exist_ok=True)
            
            # N-gram長ごとのJSONファイルを作成
            for n_gram_len, text_data in n_gram_data.items():
                # 出力ファイル名
                output_file = os.path.join(prefix_dir, f"{n_gram_len}gm.json")
                
                try:
                    with open(output_file, 'w', encoding='utf-8') as f_out:
                        json.dump(text_data, f_out, ensure_ascii=False, indent=2)
                    # print(f"  Created {output_file}")
                except Exception as e:
                    print(f"  Error writing to {output_file}: {e}")
    
    print("Processing completed.")

if __name__ == "__main__":
    main()