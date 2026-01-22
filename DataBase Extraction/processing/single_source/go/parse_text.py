import os.path

import pandas as pd
import utils.my_df_function as utils


def run(input_path, out_dir):
    # input_path = 'D:/MyCode/Code/CodeForPaper/sample_data/04_go/go_raw.obo'
    # out_dir = f'D:/MyCode/Code/CodeForPaper/result/DB_go/'
    save_name = f'GO_terms.tsv'

    # Initialize parsing result
    result_list = []

    utils.create_dir(out_dir)

    with open(input_path, 'r') as f:
        content = f.read()

        data_block_list = content.split("\n\n")
        # print(len(data_block_list))

        for data_block in data_block_list:
            line_list = data_block.split('\n')
            # print(line_list[0])

            # Initialize this database parsing result
            one_block_dict = {}

            data_type = line_list[0]
            if data_type != '[Term]':
                continue

            for i in range(1, len(line_list)):
                data_line = line_list[i]
                # print(data_line)

                key_value_list = data_line.split(': ')
                key = key_value_list[0]
                value = key_value_list[1]

                original_key = key
                key_number = 1
                while key in one_block_dict:
                    key = f'{original_key}_{key_number}'
                    key_number += 1

                d = {key: value}
                one_block_dict.update(d)

            result_list.append(one_block_dict)

    df = pd.DataFrame(result_list)

    save_path = os.path.join(out_dir, save_name)
    utils.save_df(save_path, df)
    return df

