# Change column headers to lowercase; spaces to '_'; '-' to '_';
def change_columns(df):
    column_list = df.columns.tolist()
    rename_dict = {}

    for column in column_list:

        new_column = column.lower()

        if '-' in new_column:
            new_column = new_column.replace('-', '_')

        if ' ' in new_column:
            new_column = new_column.replace(' ', '_')

        d = {column: new_column}

        rename_dict.update(d)

    df = df.rename(columns=rename_dict)

    return df

