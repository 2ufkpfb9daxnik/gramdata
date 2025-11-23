import json

def convert_txt_to_json(input_file, output_file):
    """
    emojinarabeasobi2gram.txtからデータを読み込み、JSON形式に変換する
    
    Args:
        input_file (str): 入力テキストファイルのパス
        output_file (str): 出力JSONファイルのパス
    """
    data_dict = {}
    
    try:
        # テキストファイルを読み込む
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:  # 空行をスキップ
                    continue
                    
                parts = line.split('\t')
                if len(parts) >= 2:
                    count = int(parts[0])
                    bigram = parts[1]
                    
                    # 辞書に追加
                    data_dict[bigram] = count
        
        # JSONファイルに書き出し
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, ensure_ascii=False, indent=4)
        
        print(f"{input_file}を読み込み、{output_file}に変換しました。")
        print(f"合計 {len(data_dict)} 件のデータを変換しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    input_file = "emojinarabeasobi2gram.txt"
    output_file = "emojinarabeasobi2gram.json"
    convert_txt_to_json(input_file, output_file)