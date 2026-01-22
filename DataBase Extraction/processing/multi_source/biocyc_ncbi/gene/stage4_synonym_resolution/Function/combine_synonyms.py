"""
Editor: INK
Create Time: 2024/6/3 15:24
File Name: combine_dblink.py
Function: 
"""
import numpy as np
import pandas as pd

import utils.my_df_function as until


# Execution process for merging multiple synonyms
def combine_process_multiple(row):
    synonyms_ncbi = row['synonyms_ncbi']
    synonyms_biocyc = row['synonyms_biocyc']
    accession_id = row['accession_id']

    # Convert target data to list
    if pd.isnull(synonyms_ncbi) or synonyms_ncbi == '-':
        list_synonyms_ncbi= []
    else:
        list_synonyms_ncbi = until.transform_back_to_list(synonyms_ncbi)

    if pd.isnull(synonyms_biocyc) or synonyms_biocyc == '-':
        list_synonyms_biocyc = []
    else:
        list_synonyms_biocyc = until.transform_back_to_list(synonyms_biocyc)

    if pd.isnull(accession_id) or accession_id == '-':
        list_accession_id = []
    else:
        list_accession_id = until.transform_back_to_list(accession_id)

    # print(list_synonyms_biocyc)
    # print(list_synonyms_ncbi)
    # print(list_accession_id)
    # if pd.isnull(list_synonyms_ncbi):
    #     print(synonyms_ncbi)
    #     print(True)
    final_list = list_synonyms_biocyc + list_synonyms_ncbi + list_accession_id
    final_list = list(set(final_list))

    if len(final_list) == 0:
        row['synonyms'] = np.nan
        return row

    # Determine whether to delete gene_name within it
    symbol = row['symbol']
    if pd.isnull(symbol) or symbol == '-':
        pass
    else:
        if symbol in final_list:
            final_list.remove(symbol)
    final_str = until.change_list_to_special_data(final_list)
    row['synonyms'] = final_str
    return row


# Single synonyms merge process
def combine_process_single(row):
    synonyms = row['synonyms']
    accession_id = row['accession_id']

    # Convert target data to list
    if pd.isnull(synonyms):
        list_synonyms = []
    else:
        list_synonyms = until.transform_back_to_list(synonyms)
    if pd.isnull(accession_id) or accession_id == '-':
        list_accession_id = []
    else:
        list_accession_id = until.transform_back_to_list(accession_id)

    final_list = list_synonyms + list_accession_id
    final_list = list(set(final_list))


    if len(final_list) == 0:
        row['synonyms'] = np.nan
        return row

    symbol = row['symbol']
    if pd.isnull(symbol) or symbol == '-':
        pass
    else:
        if symbol in final_list:
            final_list.remove(symbol)
    str_synonyms = until.change_list_to_special_data(final_list)
    row['synonyms'] = str_synonyms
    return row


# Start merge
def start_combine_process(df):
    columns_list = df.columns.tolist()
    if 'synonyms_ncbi' in columns_list and 'synonyms_biocyc' in columns_list and 'accession_id' in columns_list:
        df = df.apply(combine_process_multiple, axis=1)
        drop_columns = ['synonyms_ncbi', 'synonyms_biocyc', 'accession_id']
        df = df.drop(columns=drop_columns)
    elif 'synonyms' in columns_list and 'accession_id' in columns_list:
        df = df.apply(combine_process_single, axis=1)
    return df
