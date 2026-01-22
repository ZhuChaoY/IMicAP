import ast
import pandas as pd
import utils.my_df_function as utils


 # Standardize column headers
def change_columns_to_same_format(df, special_columns):
    old_columns = df.columns.tolist()

    # Column headers to lowercase
    # Replace '-' with '_'
    # Replace ' ' with '_'
    rename_dict = {}
    for column in old_columns:
        if column not in special_columns:
            # print(column)
            new_column = column.lower()
            new_column = new_column.replace('-', '_')
            new_column = new_column.replace(' ', '_')
            d = {column: new_column}
            rename_dict.update(d)
    df = df.rename(columns=rename_dict)
    return df


 # Fill missing values
def fill_na(df):
    df = df.fillna('NA_NO')
    return df

 # Handle multiple values
def deal_multiply_value(df_original, multiply_column_dict):
    """
    :param df_original:
    :param multiply_column_dict: Dictionary storing columns with multiple values
        Example:
        {
            'semicolon_cols': [],
            'semicolon_blank_cols': []
        }
    :return:
    """

    def process_row(row, semicolon_cols, semicolon_blank_cols, blank_cols, list_cols, comma_blank_cols):
        """
        Standardize processing of a row of data, handling different columns as needed
        """

        def process_data(data_value, delimiter):
            if pd.isnull(data_value) or data_value == 'NA_NO':
                return data_value

            data_list = data_value.split(delimiter)

            new_list = []
            for item in data_list:
                stripped_item = item.strip()  # Remove leading/trailing spaces
                if len(stripped_item) > 0:  # Only add non-empty strings
                    new_list.append(stripped_item)

            return utils.change_list_to_special_data(new_list)

        # Process columns with ';' delimiter
        for column in semicolon_cols:
            row[column] = process_data(row[column], ';')

        # Process columns with '; ' delimiter
        for column in semicolon_blank_cols:
            row[column] = process_data(row[column], '; ')

        # Process columns with ' ' delimiter
        for column in blank_cols:
            row[column] = process_data(row[column], ' ')

        # Process columns with ', ' delimiter
        for column in comma_blank_cols:
            row[column] = process_data(row[column], ', ')

        # Process columns stored as lists
        for column in list_cols:
            data = row[column]
            if data == 'NA_NO':
                continue
            try:
                transform_data = ast.literal_eval(data)
                if isinstance(transform_data, list):
                    row[column] = utils.change_list_to_special_data(transform_data)
            except:
                continue

        return row

    def deal_columns(df, multiply_dict):
        """
        Standardize processing of multiple columns, using apply to process all columns at once
        """

        # Multiple values joined by '; '
        list_semicolon_blank_column = []
        if 'semicolon_blank_column' in multiply_dict:
            list_semicolon_blank_column = multiply_dict.get('semicolon_blank_column')
        semicolon_blank_cols = utils.conclude_prepare_columns(list_semicolon_blank_column, df)

        # Multiple values joined by ';'
        list_semicolon_column = []
        if 'semicolon_column' in multiply_dict:
            list_semicolon_column = multiply_dict.get('semicolon_column')
        semicolon_cols = utils.conclude_prepare_columns(list_semicolon_column, df)

        # Multiple values joined by ', '
        list_comma_blank_column = []
        if 'comma_blank_column' in multiply_dict:
            list_comma_blank_column = multiply_dict.get('comma_blank_column')
        comma_blank_cols = utils.conclude_prepare_columns(list_comma_blank_column, df)

        # Multiple values joined by ' '
        list_blank_column = []
        if 'blank_column' in multiply_dict:
            list_blank_column = multiply_dict.get('blank_column')
        blank_cols = utils.conclude_prepare_columns(list_blank_column, df)

        # Multiple values stored as ['a', 'b']
        list_list_column = []
        if 'list_column ' in multiply_dict:
            list_list_column = multiply_dict.get('list_column')
        list_cols = utils.conclude_prepare_columns(list_list_column, df)

        df = df.apply(
            lambda row: process_row(row, semicolon_cols, semicolon_blank_cols, blank_cols, list_cols, comma_blank_cols),
            axis=1)

        return df

    df_new = deal_columns(
        df=df_original,
        multiply_dict=multiply_column_dict
    )

    return df_new
