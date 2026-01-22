import numpy as np
import pandas as pd
import utils.my_df_function as utils

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

        if pd.isnull(dbxref) or dbxref == 'NA_NO':
            list_dbxref = []
        else:
            list_dbxref = utils.transform_back_to_list(dbxref)
        if pd.isnull(dblink) or dblink == 'NA_NO':
            list_dblink = []
        else:
            list_dblink = utils.transform_back_to_list(dblink)

        final_list = list(set(list_dbxref + list_dblink))
        if len(final_list) == 0:
            final_str = np.nan
        else:
            final_str = utils.change_list_to_special_data(final_list)

        return final_str
    if 'DBLINKS' not in df.columns:
        df['DBLINKS'] = np.nan
    df['DBLINKS'] = df.apply(lambda x: combine_process(x['dbxrefs'], x['DBLINKS']), axis=1)
    drop_column = ['dbxrefs']
    df = df.drop(columns=drop_column)
    return df

