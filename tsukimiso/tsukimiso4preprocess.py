import json
import os

def convert_txt_to_json(input_path, output_path):
    """
    テキストファイルからデータを読み取り、JSONファイルに変換する
    
    Args:
        input_path (str): 入力テキストファイルのパス
        output_path (str): 出力JSONファイルのパス
    """
    # 結果を格納する辞書
    result = {}
    
    try:
        with open(input_path, 'r', encoding='utf-8') as file:
            for line in file:
                # コメント行やファイルパス行をスキップ
                if line.strip().startswith('//') or not line.strip():
                    continue
                
                # タブで分割して4文字のひらがなと出現数を取得
                parts = line.strip().split('\t')
                if len(parts) == 2:
                    ngram, count = parts
                    # 出現数を整数に変換
                    try:
                        result[ngram] = int(count)
                    except ValueError:
                        print(f"警告: '{count}' を整数に変換できません。行: '{line.strip()}'")
    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")
        return
    
    # JSONファイルに書き出し
    try:
        with open(output_path, 'w', encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False, indent=4)
        print(f"{output_path} にJSONデータを保存しました。")
    except Exception as e:
        print(f"ファイル書き込みエラー: {e}")

if __name__ == "__main__":
    # 入力ファイルと出力ファイルのパスを定義
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(current_dir, "", "tsukimiso4gram.txt")
    output_file = os.path.join(current_dir, "", "tsukimiso4gram.json")
    
    # 変換を実行
    convert_txt_to_json(input_file, output_file)
