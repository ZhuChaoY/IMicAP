import os.path

import pandas as pd

import utils.my_df_function as utils


class ValueNormalizer:

    def __init__(self, output_dir, df_source):
        self.output_dir = output_dir
        self.df_source = df_source
        self.record_dir = os.path.join(self.output_dir, 'Record')
        utils.create_dir(self.record_dir)

    # Scan each column, verify if multi-values exist
    def multiple_value_scan(self, df):

        # Initialize empty DataFrame for storing results
        result_df = pd.DataFrame()

        # Traverse each column
        for column in df.columns:
            # Filter rows containing '|'
            filtered = df[df[column].str.contains('|', regex=False)].copy()
            if not filtered.empty:
                # Keep at most 5 rows as samples
                filtered = filtered.head(5)
                # Add new column appear_columns to filtered rows
                filtered['appear_columns'] = column
                # Add result to final DataFrame
                result_df = pd.concat([result_df, filtered])

        # Reset index
        result_df = result_df.reset_index(drop=True)

        print(result_df)

        save_name = f'multiple_value_scan_result.tsv'
        save_path = os.path.join(self.record_dir, save_name)
        utils.save_df(save_path, result_df)

    def start_filter_process(self, df):

        record_dict_list = []

        def deal_multiple_value(data):

            if pd.isnull(data):
                return data
            if data == 'NA_NO':
                return data

            data_list = data.split('|')
            data = utils.change_list_to_special_data(data_list)
            return data

        special_column_list = [
            'Synonyms',
            'dbXrefs',
            'Other_designations'
        ]
        for column in special_column_list:
            df[column] = df[column].apply(lambda x: deal_multiple_value(x))

        rename_dict = {}
        for column in df.columns:
            new_columns = column.replace(' ', '_')
            new_columns = new_columns.replace('-', '_')
            new_columns = new_columns.lower()

            rename_dict[column] = new_columns
        df = df.rename(columns=rename_dict)

        save_dir = os.path.join(self.output_dir, 'Data')
        utils.create_dir(save_dir)
        save_name = f'ncbi_info_normalize_result.tsv'
        save_path = os.path.join(save_dir, save_name)
        utils.save_df(save_path, df)

        d = {
            'file_name': save_name,
            'file_count': len(df)
        }
        record_dict_list.append(d)

        df_record = pd.DataFrame(record_dict_list)
        save_name_record = 'record.tsv'
        record_dir = self.record_dir
        save_path = os.path.join(record_dir, save_name_record)
        utils.save_df(save_path, df_record)

    def run(self):
        self.multiple_value_scan(df=self.df_source)
        self.start_filter_process(df=self.df_source)
