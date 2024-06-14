import unittest
import os
import pickle
import csv
import json
from batch_processing import load_data, save_results, main, load_config

class TestBatchProcessing(unittest.TestCase):

    def setUp(self):
        # テスト用の入力データ
        self.test_data = [
            ('A', 10),
            ('A', 20),
            ('A', 30),
            ('B', 15),
            ('B', 25),
            ('C', 35),
            ('C', 45),
            ('C', 55)
        ]
        
        # テスト用の入力ファイル
        self.input_file = 'test_input.csv'
        with open(self.input_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in self.test_data:
                writer.writerow(row)
        
        # テスト用の設定ファイル
        self.config_file = 'test_config.json'
        self.config_data = {
            "input_file": self.input_file,
            "output_file": 'test_output.csv',
            "state_file": 'test_state.pkl'
        }
        with open(self.config_file, 'w') as jsonfile:
            json.dump(self.config_data, jsonfile)

    def tearDown(self):
        # テストファイルを削除
        if os.path.exists(self.input_file):
            os.remove(self.input_file)
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
        if os.path.exists(self.config_data['output_file']):
            os.remove(self.config_data['output_file'])
        if os.path.exists(self.config_data['state_file']):
            os.remove(self.config_data['state_file'])

    def test_load_data(self):
        data = load_data(self.input_file)
        self.assertEqual(data, self.test_data)
    
    def test_save_results(self):
        results = [('A', 60), ('B', 40), ('C', 135)]
        output_file = self.config_data['output_file']
        save_results(results, output_file)
        
        with open(output_file, newline='') as csvfile:
            reader = csv.reader(csvfile)
            output_data = [(row[0], int(row[1])) for row in reader]
        
        self.assertEqual(results, output_data)
    
    def test_load_config(self):
        config = load_config(self.config_file)
        self.assertEqual(config, self.config_data)

    def test_main(self):
        main(self.config_data)
        
        results = [('A', 60), ('B', 40), ('C', 135)]
        with open(self.config_data['output_file'], newline='') as csvfile:
            reader = csv.reader(csvfile)
            output_data = [(row[0], int(row[1])) for row in reader]
        
        self.assertEqual(results, output_data)
        self.assertFalse(os.path.exists(self.config_data['state_file']))

if __name__ == '__main__':
    unittest.main()
