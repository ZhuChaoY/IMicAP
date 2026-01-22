"""
Editor: INK
Create Time: 2024/8/16 15:00
File Name: build_temp_table.py
Function: 
"""
import numpy as np

import utils.my_df_function as until


def build_temp_table(df):
    target_column_list = [
        'strains_tax_id',
        'NCBI_gene_id',
        'gene_biocyc_id',
        'dbxrefs',
        'description',
        'other_designations',
        'UniProt_id',
        'product_name',
        'gene_name',
        'accession_id',
        'product',
    ]

    column_list = df.columns.tolist()
    print(column_list)

    not_exist_column = []
    for column in target_column_list:
        if column not in column_list:
            print(column)
            not_exist_column.append(column)
            # target_column_list.remove(column)
    for column in not_exist_column:
        target_column_list.remove(column)

    df_temp = df[target_column_list].copy()

    for column in not_exist_column:
        df[column] = np.nan

    return df_temp
