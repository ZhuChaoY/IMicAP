import os.path

import utils.my_df_function as utils

import gzip
import pandas as pd
import os


def run(input_path, out_dir):
    # input_path = 'D:/MyCode/Code/CodeForPaper/sample_data/04_go/go_raw.obo.gz'
    # out_dir = f'D:/MyCode/Code/CodeForPaper/result/DB_go/'
    save_name = f'GO_terms.tsv'

    # Initialize parsing result
    result_list = []

    utils.create_dir(out_dir)

    # 判断是否为 gzip 文件
    if input_path.endswith('.gz'):
        # 使用 gzip 打开压缩文件
        with gzip.open(input_path, 'rt', encoding='utf-8') as f:  # 'rt' 表示以文本模式读取
            content = f.read()
    else:
        # 保持原有逻辑
        with open(input_path, 'r') as f:
            content = f.read()

    data_block_list = content.split("\n\n")
    print(content)
    print(len(data_block_list))

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

            # 跳过空行
            if not data_line:
                continue

            key_value_list = data_line.split(': ', 1)  # 使用 maxsplit=1 只分割第一个冒号
            if len(key_value_list) != 2:
                continue  # 跳过格式不正确的行

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

