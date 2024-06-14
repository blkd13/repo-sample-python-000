import csv
import logging
import os
import pickle
import argparse
import json

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(input_file):
    """入力ファイルからデータを読み込む"""
    data = []
    try:
        with open(input_file, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                key, value = row
                data.append((key, int(value)))
    except Exception as e:
        logging.error(f"データの読み込み中にエラーが発生しました: {e}")
        raise
    return data

def save_results(results, output_file):
    """結果を出力ファイルに保存する"""
    try:
        with open(output_file, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for key, total in results:
                writer.writerow([key, total])
    except Exception as e:
        logging.error(f"結果の保存中にエラーが発生しました: {e}")
        raise

def load_config(config_file):
    """設定ファイルを読み込む"""
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
        return config
    except Exception as e:
        logging.error(f"設定ファイルの読み込み中にエラーが発生しました: {e}")
        raise

def main(config):
    input_file = config['input_file']
    output_file = config['output_file']
    state_file = config['state_file']

    # 状態の初期化
    state = {
        'current_key': None,
        'current_sum': 0,
        'index': 0
    }

    # 状態の読み込み
    if os.path.exists(state_file):
        with open(state_file, 'rb') as f:
            state = pickle.load(f)
        logging.info(f'状態を復元しました: {state}')

    # 入力データの読み込み
    data = load_data(input_file)
    results = []

    # バッチ処理
    try:
        for i in range(state['index'], len(data)):
            key, value = data[i]
            if state['current_key'] is None:
                # 最初のキーを設定
                state['current_key'] = key

            if key != state['current_key']:
                # キーが変わった場合、前のキーの合計を出力
                logging.info(f'Key: {state["current_key"]}, Sum: {state["current_sum"]}')
                results.append((state['current_key'], state['current_sum']))
                # 新しいキーに切り替え、集計をリセット
                state['current_key'] = key
                state['current_sum'] = 0

            # 現在のキーの値を集計
            state['current_sum'] += value
            state['index'] = i + 1

        # 最後のキーの合計を結果に追加
        if state['current_key'] is not None:
            logging.info(f'Key: {state["current_key"]}, Sum: {state["current_sum"]}')
            results.append((state['current_key'], state['current_sum']))

        # 結果を出力ファイルに保存
        save_results(results, output_file)

        # 状態保存ファイルを削除（全ての処理が完了したため）
        if os.path.exists(state_file):
            os.remove(state_file)
        logging.info('バッチ処理が完了しました。状態ファイルを削除しました。')

    except Exception as e:
        # エラーハンドリングと状態の保存
        logging.error(f'エラーが発生しました: {e}')
        with open(state_file, 'wb') as f:
            pickle.dump(state, f)
        logging.info(f'状態を保存しました: {state}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='バッチ処理プログラム')
    parser.add_argument('config_file', help='設定ファイルのパス (JSON)')

    args = parser.parse_args()
    config = load_config(args.config_file)
    main(config)
