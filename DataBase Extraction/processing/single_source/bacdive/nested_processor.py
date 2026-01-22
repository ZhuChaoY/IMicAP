"""
Editor: INK
Create Time: 2025/2/20 10:22
File Name: nested_processor.py
Function: 
"""

import utils.my_df_function as utils
import os
import json
import pandas as pd


def flatten_process(pre_key, data_value, join_sep='.'):
    result = {}

    if isinstance(data_value, dict):  # Handle dict
        for k, v in data_value.items():
            # Recursively check each key's value
            new_key = f"{pre_key}.{k}"
            dict_exact_dict = flatten_process(pre_key=new_key, data_value=v)
            result.update(dict_exact_dict)
    elif isinstance(data_value, list):  # Handle list
        # Check if any element in the list is a dict
        if any(isinstance(item, dict) for item in data_value):
            i_count = 0  # Initialize level counter
            for i, item in enumerate(data_value):
                pure_str_data = []  # Used to store data other than dict
                if isinstance(item, dict):
                    # Add level counter, recursively check each key's value
                    if i_count == 0:
                        new_key = f"{pre_key}"
                    else:
                        new_key = f"{pre_key}.{i_count}"

                    dict_exact_dict = flatten_process(pre_key=new_key, data_value=item)
                    result.update(dict_exact_dict)
                    i_count += 1  # Only update level counter when element is dict
                else:
                    if item is not None:
                        item = str(item)
                    pure_str_data.append(item)
                    list_exact_dict = {
                        pre_key: pure_str_data
                    }
                    result.update(list_exact_dict)
        # If not present, the list does not need further parsing
        else:
            if data_value is not None:
                data_value = str(data_value)
            list_exact_dict = {
                pre_key: data_value
            }
            result.update(list_exact_dict)

    else:  # Handle other data types
        if data_value is not None:
            data_value = str(data_value)

        normal_result_dict = {
            pre_key: data_value
        }
        result = normal_result_dict

    return result


def run(input_dir, output_dir):
    utils.create_dir(output_dir)

    # Store data results
    save_dir_data = os.path.join(output_dir, 'data')
    utils.create_dir(save_dir_data)

    # Store column name results
    save_dir_col = os.path.join(output_dir, 'columns')
    utils.create_dir(save_dir_col)

    file_list = os.listdir(input_dir)

    # Initialize final result
    final_result = {}

    for file in file_list:
        # Sequentially parse each strain's JSON file and aggregate the data
        # if '100884_res' not in file:
        #     continue
        # if file != '871664_res1.json':
        #     continue
        print(file)

        data_path = os.path.join(input_dir, file)
        with open(data_path, 'r', encoding='utf-8') as f:
            data_json = json.load(f)

            bacdive_id = 'NA_NO'
            if data_json.get('General') is not None:
                if data_json.get('General').get('01_bacdive-ID') is not None:
                    bacdive_id = data_json.get('General').get('01_bacdive-ID')

            # The first-level data forms a separate table result
            for first_label in data_json:
                data_json_second = data_json.get(first_label)
                basic_d = {
                    'source_file': file,
                    'bacdive_id_exact': bacdive_id
                }  # Validation info

                # Get the parsed result for the first-level label of this strain
                exact_res_d = flatten_process(pre_key=first_label, data_value=data_json_second)
                exact_res_d.update(basic_d)

                # Aggregate parsed results {label1: []}
                if first_label in final_result:
                    df_old = final_result.get(first_label)
                    df_new = pd.DataFrame([exact_res_d])
                    df_old = pd.concat([df_old, df_new])
                else:
                    df_old = pd.DataFrame([exact_res_d])
                d = {
                    first_label: df_old
                }
                final_result.update(d)

    for first_label in final_result:
        # second_label_res_list = final_result.get(first_label)
        # df = pd.DataFrame(second_label_res_list)
        df = final_result.get(first_label)
        # Store tsv data results
        save_name = f'{first_label}.tsv'
        save_path = os.path.join(save_dir_data, save_name)
        utils.save_df(save_path, df)

        # Store column names
        columns_list = df.columns.to_list()
        save_path_columns = os.path.join(save_dir_col, save_name)
        utils.save_df(save_path, df)
        with open(save_path_columns, 'w+') as f:
            for col in columns_list:
                f.write(col + '\n')

