"""
Editor: INK
Create Time: 2024/6/3 16:18
File Name: combine_dbxrefs_dblinks.py
Function: 
"""
import numpy as np
import pandas as pd

import utils.my_df_function as until


# Handle multi-value format of dbXref
def deal_dbXref_multiply(dbxref):
    if pd.isnull(dbxref) or dbxref == '-':
        return []
    else:
        list_dbxref = dbxref.split('|')
        final_dbxref_list = []
        for item in list_dbxref:
            new_item = item.replace(':', ': ')
            final_dbxref_list.append(new_item)
        return final_dbxref_list


# Start merge process
def start_combine_process(df):
    def combine_process(dbxref, dblink):

        if pd.isnull(dbxref) or dbxref == '-':
            list_dbxref = []
        else:
            list_dbxref = until.transform_back_to_list(dbxref)
        if pd.isnull(dblink) or dblink == '-':
            list_dblink = []
        else:
            list_dblink = until.transform_back_to_list(dblink)

        final_list = list(set(list_dbxref + list_dblink))
        if len(final_list) == 0:
            final_str = np.nan
        else:
            final_str = until.change_list_to_special_data(final_list)

        return final_str

    df['dblinks'] = df.apply(lambda x: combine_process(x['dbxrefs'], x['dblinks']), axis=1)
    drop_columns = ['dbxrefs']
    df =df.drop(columns=drop_columns)

    return df
