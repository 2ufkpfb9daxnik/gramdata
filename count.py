import os
import json
import glob

def count_json_occurrences():
    # カレントディレクトリ内のすべてのJSONファイルを取得
    json_files = glob.glob('*.json')
    
    results = {}
    
    # 各JSONファイルを処理
    for file_name in json_files:
        try:
            with open(file_name, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
                # JSONデータが辞書型であることを想定
                if isinstance(data, dict):
                    # 各キーと値を処理
                    for key, value in data.items():
                        if isinstance(value, (int, float)):
                            if file_name not in results:
                                results[file_name] = 0
                            results[file_name] += value
                # JSONデータがリスト型である場合
                elif isinstance(data, list):
                    count = len(data)
                    results[file_name] = count
                    
        except Exception as e:
            print(f"エラー: {file_name}を処理中に問題が発生しました - {str(e)}")
    
    # 結果をcount.txtに書き込む
    with open('count.txt', 'w', encoding='utf-8') as output_file:
        for file_name, count in results.items():
            output_file.write(f"{file_name} {count}\n")
    
    print("集計が完了し、結果がcount.txtに保存されました。")

if __name__ == "__main__":
    count_json_occurrences()