"""
Editor: INK
Create Time: 2024/7/2 16:55
File Name: conclude_none_table.py
Function: 
"""
import pandas as pd
from .change_column_to_lower import change_columns
import utils.my_df_function as until


def conclude_process(df_biocyc, df_ncbi, save_dir, li_shan_id, now_time):
    path_merge_result = save_dir + '1st_' + li_shan_id + '_gene_ncbi_bio_' + now_time + '.tsv'
    
    print(len(df_biocyc))
    print(len(df_ncbi))
    if len(df_ncbi) == 0:
        check_path = save_dir + 'There is no data in NCBI.txt'
        with open(check_path, 'w+') as f:
            pass
        merge_df = pd.concat([df_biocyc, df_ncbi])
        merge_df = change_columns(merge_df)
        until.save_df(path_merge_result, merge_df)
    if len(df_biocyc) == 0:
        check_path = save_dir + 'There is no data in BioCyc.txt'
        with open(check_path, 'w+') as f:
            pass
        merge_df = pd.concat([df_biocyc, df_ncbi])
        merge_df = change_columns(merge_df)
        until.save_df(path_merge_result, merge_df)

