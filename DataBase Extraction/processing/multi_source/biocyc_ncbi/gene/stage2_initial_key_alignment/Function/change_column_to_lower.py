"""
Editor: INK
Create Time: 2024/6/1 12:26
File Name: change_column_to_lower.py
Function: 
"""
import utils.my_df_function as until


# Change column headers to lowercase; spaces to '_'; '-' to '_';
def change_columns(df):
    column_list = df.columns.tolist()
    rename_dict = {}

    for column in column_list:

        if column == 'UniProt-ID':
            new_column = 'UniProt_id'
            d = {column: new_column}
            rename_dict.update(d)
            continue

        new_column = column.lower()

        if '-' in new_column:
            new_column = new_column.replace('-', '_')

        if ' ' in new_column:
            new_column = new_column.replace(' ', '_')

        d = {column: new_column}

        rename_dict.update(d)

    df = df.rename(columns=rename_dict)

    return df

