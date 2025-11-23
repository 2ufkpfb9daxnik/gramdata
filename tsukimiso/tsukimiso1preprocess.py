import json

def convert_txt_to_json(input_file, output_file):
    """
    tsukimiso1gram.txtファイルを読み込み、JSONファイルに変換する関数
    
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
                
            # 文字と出現回数に分割
            parts = line.strip().split()
            if len(parts) >= 2:
                char = parts[0]
                count = int(parts[1])
                data_dict[char] = count
    
    # JSONファイルに書き出し
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data_dict, f, ensure_ascii=False, indent=4)
    
    print(f"{input_file}を読み込み、{output_file}に変換しました。")

if __name__ == "__main__":
    input_file = "tsukimiso1gram.txt"
    output_file = "tsukimiso1gram.json"
    convert_txt_to_json(input_file, output_file)