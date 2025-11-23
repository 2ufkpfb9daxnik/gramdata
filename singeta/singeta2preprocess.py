import json
import csv

def convert_csv_to_json(input_file, output_file):
    """
    singeat2.csvからデータを読み込み、JSON形式に変換する
    
    Args:
        input_file (str): 入力CSVファイルのパス
        output_file (str): 出力JSONファイルのパス
    """
    data_dict = {}
    
    try:
        # CSVファイルを読み込む
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                if len(row) >= 3:
                    count = int(row[0])
                    char1 = row[1]
                    char2 = row[2]
                    
                    # 2グラムの文字列を作成（2文字連結）
                    bigram = char1 + char2
                    
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
    input_file = "singeta2.csv"
    output_file = "singeta2gram.json"
    convert_csv_to_json(input_file, output_file)