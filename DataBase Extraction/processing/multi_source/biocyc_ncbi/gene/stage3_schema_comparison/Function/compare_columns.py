"""
Editor: INK
Create Time: 2024/6/3 11:12
File Name: compare_columns.py
Function: 
"""
import pandas as pd

import utils.my_df_function as until


# Supplement data
def supply_data(df, base_column, support_column):
    def supply_process(base_data, support_data):
        if pd.isnull(base_data) or base_data == '-':
            return support_data
        else:
            return base_data

    df[base_column] = df.apply(lambda x: supply_process(x[base_column], x[support_column]), axis=1)
    return df


# Compare if two columns are completely different
def compare_process(df, base_column, support_column, save_dir, li_shan_tax_id):
    # Check if column corresponding to base_column is all empty
    # print(df.columns)
    if df[base_column].isnull().all:
        return df

    # Get part where both columns are not empty
    # print(base_column)
    df_compare = df[df[base_column].notnull()].copy()
    # print(len(df_compare))
    # save_path = './' + base_column + '.tsv'
    # until.save_df(save_path, df_compare)

    df_compare = df_compare[df_compare[support_column].notnull()]
    df_compare = df_compare[df_compare[base_column] != '-']
    df_compare = df_compare[df_compare[support_column] != '-']

    # Get part where two columns are different
    df_different = df_compare[df_compare[base_column] != df_compare[support_column]]

    # If completely identical
    if len(df_different) == 0:
        condition_name = base_column + 'and' + support_column + 'Completely consistent..txt'
        path_condition = save_dir + condition_name
        with open(path_condition, 'w+') as f:
            pass
        # Fill data
        df = supply_data(df, base_column, support_column)

        # Delete useless columns
        drop_column = [support_column]
        df = df.drop(columns=drop_column)
        return df

    # If not completely identical
    else:
        data_time = until.get_now_time()
        condition_name = base_column + 'and' + support_column + 'Not completely consistent..txt'
        path_condition = save_dir + condition_name
        with open(path_condition, 'w+') as f:
            pass

        file_name = li_shan_tax_id + '_ncbi_gene_' + base_column + '_' + support_column + '_info_' + data_time + '.tsv'
        save_path = save_dir + file_name
        until.save_df(save_path, df_different)

        return df
