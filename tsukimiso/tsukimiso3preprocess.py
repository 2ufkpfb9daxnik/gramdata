import json
import re

def convert_txt_to_json(input_file, output_file):
    """
    tsukimiso3gram.txtファイルを読み込み、JSONファイルに変換する関数
    
    Args:
        input_file (str): 入力ファイルのパス
        output_file (str): 出力ファイルのパス
    """
    data_dict = {}
    
    # ファイルを読み込み、各行を処理
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            # 空行をスキップ
            if line.strip() == '':
                continue
            
            # "文字列" "出現回数" という形式をパース
            # タブや余分な "、スペースなども考慮した正規表現
            match = re.match(r'"([^"]+)"\s*"(\d+)[^"]*"', line)
            if match:
                # タブ文字を削除して保存
                char_seq = match.group(1).replace('\t', '')
                count = int(match.group(2))
                data_dict[char_seq] = count
    
    # JSONファイルに書き出し
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data_dict, f, ensure_ascii=False, indent=4)
    
    print(f"{input_file}を読み込み、{output_file}に変換しました。")
    print(f"合計 {len(data_dict)} 件のデータを変換しました。")

if __name__ == "__main__":
    input_file = "tsukimiso3gram.txt"
    output_file = "tsukimiso3gram.json"
    convert_txt_to_json(input_file, output_file)