import json

def convert_txt_to_json(input_file, output_file):
    """
    emojinarabeasobi3gram.txtからデータを読み込み、JSON形式に変換する
    
    Args:
        input_file (str): 入力テキストファイルのパス
        output_file (str): 出力JSONファイルのパス
    """
    data_dict = {}
    duplicates = []
    
    try:
        # テキストファイルを読み込む
        with open(input_file, 'r', encoding='utf-8') as f:
            line_num = 0
            for line in f:
                line_num += 1
                line = line.strip()
                if not line:  # 空行をスキップ
                    continue
                    
                parts = line.split('\t')
                if len(parts) >= 2:
                    try:
                        count = int(parts[0])
                        trigram = parts[1]
                        
                        # 重複チェック
                        if trigram in data_dict:
                            duplicates.append((trigram, line_num, data_dict[trigram], count))
                            # 出現回数を合計する（または選択肢として）
                            data_dict[trigram] += count
                        else:
                            # 辞書に追加
                            data_dict[trigram] = count
                    except ValueError:
                        print(f"警告: 行 {line_num} のデータ形式が不正です: {line}")
        
        # 重複があれば報告
        if duplicates:
            print(f"注意: {len(duplicates)}個の重複した3グラムが見つかりました:")
            for trigram, line_num, old_count, new_count in duplicates:
                print(f"  - \"{trigram}\" が行 {line_num} で重複 (元の値: {old_count}, 新しい値: {new_count}, 合計: {data_dict[trigram]})")
        
        # JSONファイルに書き出し
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, ensure_ascii=False, indent=4)
        
        print(f"{input_file}を読み込み、{output_file}に変換しました。")
        print(f"合計 {len(data_dict)} 件のデータを変換しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    input_file = "emojinarabeasobi3gram.txt"
    output_file = "emojinarabeasobi3gram.json"
    convert_txt_to_json(input_file, output_file)