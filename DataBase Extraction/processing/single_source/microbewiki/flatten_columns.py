"""
Editor: INK
Create Time: 2025/1/21 16:13
File Name: flatten_columns.py
Function: 
"""
import json
import os

import pandas as pd

import utils.my_df_function as utils


class MicrobeWikiFlatten:

    def __init__(self, input_dir, output_dir, save_name):
        self.input_dir = input_dir
        self.out_dir = output_dir
        utils.create_dir(self.out_dir)

        self.save_name = save_name


    def flatten_dict(self, d, parent_key='', sep='.'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):  # If dictionary, recursively expand
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):  # If list
                # Check if dictionaries exist in list
                if any(isinstance(item, dict) for item in v):
                    # If dictionaries exist, expand dictionary part
                    for i, item in enumerate(v):
                        if isinstance(item, dict):  # If dictionary, expand
                            items.extend(self.flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
                        else:  # If not dictionary, directly retain
                            items.append((f"{new_key}{sep}{i}", item))
                else:  # If dictionaries not exist in list, directly retain entire list
                    items.append((new_key, v))
            else:  # Other types directly retain
                items.append((new_key, v))
        return dict(items)

    def run(self):
        data_dir = self.input_dir
        save_dir = self.out_dir

        utils.create_dir(save_dir)
        file_list = os.listdir(data_dir)

        all_file_dict_list = []

        for file in file_list:
            data_path = os.path.join(data_dir, file)
            with open(data_path, 'r', encoding='utf-8') as f:
                data_json = json.load(f)

                basic_d = {
                    'source_file': file
                }
                exact_res_d = self.flatten_dict(data_json)
                exact_res_d.update(basic_d)
                all_file_dict_list.append(exact_res_d)

        df = pd.DataFrame(all_file_dict_list)
        save_name = self.save_name
        save_path = os.path.join(save_dir, save_name)
        utils.save_df(save_path, df)