"""
Editor: INK
Create Time: 2024/6/4 11:31
File Name: deal_empty_value.py
Function: 
"""
import pandas as pd

import utils.my_df_function as until


# Convert empty values of specified columns
def deal_target_columns(df):
    # For target columns, replace - with NA_NO, replace empty values with NA_NO
    def replace_signal(data):

        if pd.isnull(data):
            return data
        elif data == '-':
            return 'NA_NO'
        else:
            return data

    for column in df.columns.tolist():
        if column != 'transcription_direction':
            df[column] = df[column].apply(lambda x: replace_signal(x))

    df = df.fillna('NA_NO')
    return df


# Replace empty values of all columns with specified character
def replace_with_target_str(df):
    df = df.fillna('NA_NO')
    return df
