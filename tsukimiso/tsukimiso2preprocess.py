import json
import os

def convert_2gram_to_json(input_path, output_path):
    """
    2gramのテキストファイルからデータを読み取り、JSONファイルに変換する

    Args:
        input_path (str): 入力テキストファイル（tsukimiso2gram.txt）のパス
        output_path (str): 出力JSONファイル（tsukimiso2gram.json）のパス
    """
    # 結果を格納する辞書
    result = {}

    line_count = 0
    valid_entries = 0
    error_count = 0

    try:
        with open(input_path, 'r', encoding='utf-8') as file:
            for line in file:
                line_count += 1
                # 空行や不正な行をスキップ
                line = line.strip()
                if not line or line.startswith('//'):
                    continue

                # タブで分割して処理
                parts = line.split('\t')
                if len(parts) >= 3:  # 3つ以上のパーツがある可能性を考慮
                    first_char, second_char, count_str = parts[0], parts[1], parts[2]

                    # 出現回数を整数に変換（余分な文字がついている可能性を考慮）
                    count = None
                    try:
                        count = int(count_str)
                    except ValueError:
                        # 数字部分だけを抽出して変換を試みる
                        import re
                        number_match = re.search(r'\d+', count_str)
                        if number_match:
                            try:
                                count = int(number_match.group(0))
                            except ValueError:
                                print(f"警告: 行 {line_count} - '{count_str}' から数値を抽出できません。行: '{line}'")
                                error_count += 1
                                continue
                        else:
                            print(f"警告: 行 {line_count} - '{count_str}' に数値が見つかりません。行: '{line}'")
                            error_count += 1
                            continue

                    # 1文字目と2文字目を結合
                    combined = first_char + second_char

                    # 辞書に追加
                    result[combined] = count
                    valid_entries += 1
                else:
                    print(f"警告: 行 {line_count} - 正しくないフォーマットの行: '{line}'")
                    error_count += 1
    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")
        return

    # JSONファイルに書き出し
    try:
        with open(output_path, 'w', encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False, indent=4)
        print(f"{output_path} にJSONデータを保存しました。")
        print(f"処理した行数: {line_count}、有効なエントリ数: {valid_entries}、エラー数: {error_count}")
    except Exception as e:
        print(f"ファイル書き込みエラー: {e}")

if __name__ == "__main__":
    # 入力ファイルと出力ファイルのパスを定義
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(current_dir, "tsukimiso2gram.txt")
    output_file = os.path.join(current_dir, "tsukimiso2gram.json")

    # 変換を実行
    convert_2gram_to_json(input_file, output_file)
    print("変換処理を完了しました。")
