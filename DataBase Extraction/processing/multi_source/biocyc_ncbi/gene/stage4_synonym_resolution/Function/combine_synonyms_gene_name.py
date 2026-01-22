"""
Editor: INK
Create Time: 2024/6/25 11:14
File Name: combine_synonyms.py
Function: 
"""
import numpy as np
import pandas as pd

import utils.my_df_function as until


def combine_process(row):
    symbol = row['symbol']
    gene_name = row['gene_name']
    synonyms = row['synonyms']

    if pd.isnull(synonyms) or synonyms == '-':
        list_synonyms = []
    else:
        list_synonyms = until.transform_back_to_list(synonyms)

    final_list = list_synonyms
    if pd.notna(symbol) and symbol != '-':
        if pd.notna(gene_name) and gene_name != '-':
            if symbol != gene_name:
                if gene_name not in final_list:
                    final_list.append(gene_name)

    if len(final_list) == 0:
        row['synonyms'] = np.nan
    else:
        final_str = until.change_list_to_special_data(final_list)
        row['synonyms'] = final_str
    return row


def start_combine_process(df):
    column_list = df.columns.tolist()

    if 'symbol' in column_list and 'gene_name' in column_list:
        df = df.apply(combine_process, axis=1)
        drop_column = ['gene_name']
        df = df.drop(columns=drop_column)

    return df
