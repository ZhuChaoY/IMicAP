"""
Editor: INK
Create Time: 2024/6/3 15:17
File Name: compare_symbol_gene_name.py
Function: 
"""
import pandas as pd

import utils.my_df_function as until


def start_filter(df, save_dir):
    # Filter out data where both columns have values but are different
    filter_different(df, save_dir)

    # When gene_name column has value and symbol column is empty: supplement gene_name value to symbol
    df['symbol'] = df.apply(
        lambda x: supply_symbol_process(x['gene_name'], x['symbol']), axis=1
    )
    return df


# Filter out data where both columns have values but are different
def filter_different(df, save_dir):
    keep_columns = ['geneid', 'symbol', 'gene_name']
    df_compare = df[keep_columns]
    df_compare = df_compare[df_compare['symbol'].notnull() & df_compare['gene_name'].notnull()]
    df_compare = df_compare[df_compare['symbol'] != '-']
    df_compare = df_compare[df_compare['gene_name'] != '-']

    df_different = df_compare[df_compare['symbol'] != df_compare['gene_name']]
    if len(df_different) != 0:
        save_path = save_dir + 'different_symbol_gene_name.tsv'
        until.save_df(save_path, df_different)


# When gene_name column has value and symbol column is empty: supplement gene_name value to symbol
def supply_symbol_process(gene_name, symbol):
    if pd.isnull(symbol):
        return gene_name
    else:
        return symbol
