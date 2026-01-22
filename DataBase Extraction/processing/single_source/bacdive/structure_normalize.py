import os
import re
import pandas as pd
import utils.my_df_function as utils
import openpyxl

class BacDiveStructureNormalize:
    def __init__(self, struct_dir, flatten_res_dir, output_dir):
        self.struct_dir = struct_dir
        self.flatten_res_dir = flatten_res_dir
        self.output_dir = output_dir

    # Get given column name structure information
    @staticmethod
    def get_struct_columns_dict(df):
        columns_not_appear_current = df[df['OC'].isnull()].copy()  # Columns not existing in current data
        df = df[df['OC'].notnull()]
        append_column = columns_not_appear_current['SC'].tolist()  # Columns that final result needs to add

        # Initialize maximum count
        max_count = None

        # Initialize column rename dictionary
        temp_dict = df.set_index('OC')['SC'].to_dict()
        according_dict = {}

        # Get maximum count and column rename dict
        OC_columns_list = df['OC'].tolist()
        for oc_column in OC_columns_list:
            pattern = r'\.(\d+)\.'
            # print(oc_column)

            # Use re.search to find match
            match = re.search(pattern, oc_column)
            if match:
                # Extract matched number
                number = match.group(1)
                number = int(number)
                # print(f"Matched number is: {number}")

                # Update maximum count
                if max_count is None:
                    max_count = number
                else:
                    if number > max_count:
                        max_count = number

                # Replace number with special character
                new_column = oc_column.replace(f'.{number}.', '.LiShanMAX.')
                sc_column = temp_dict.get(oc_column)
                d = {
                    new_column: sc_column
                }
                according_dict.update(d)
                # print(d)
            else:
                sc_column = temp_dict.get(oc_column)
                d = {
                    oc_column: sc_column
                }
                according_dict.update(d)

        return append_column, max_count, according_dict

    # Extract specified columns to form new df, concatenate new df to form final result
    @staticmethod
    def build_final_df(df, append_column, max_count, according_dict):

        # Initialize new columns
        for column in append_column:
            if column not in df.columns:
                df[column] = None

        permanent_column_list = append_column  # Initialize permanent columns
        ascending_column_list = []  # Initialize ascending columns
        final_df = pd.DataFrame()  # Initialize final split result

        # Build permanent columns and ascending columns
        for oc_column in according_dict:
            if 'LiShanMAX' in oc_column:
                ascending_column_list.append(oc_column)
            else:
                permanent_column_list.append(oc_column)

        # Maximum count empty, meaning only permanent columns exist
        if max_count is None:
            final_df = df.reindex(columns=permanent_column_list)

            # Rename data
            if len(according_dict) > 0:
                final_df = final_df.rename(columns=according_dict)

        else:
            for i in range(0, max_count + 1):
                rename_columns_dict = {}  # Initialize column rename dictionary
                for oc_column in permanent_column_list:
                    if oc_column in according_dict:
                        d = {
                            oc_column: according_dict.get(oc_column)
                        }
                        rename_columns_dict.update(d)

                # Replace special symbols with specific count
                ascending_columns_with_number = []  # Initialize ascending column replacement result
                for oc_column in ascending_column_list:

                    # Generate ascending column replacement result
                    if i == 0:
                        new_oc_column = oc_column.replace('.LiShanMAX.', '.')
                    else:
                        new_oc_column = oc_column.replace('.LiShanMAX.', f'.{i}.')
                    ascending_columns_with_number.append(new_oc_column)

                    # Build new rename dictionary
                    sc_column = according_dict.get(oc_column)
                    d = {
                        new_oc_column: sc_column
                    }
                    rename_columns_dict.update(d)

                # Build a df's final column header situation
                final_columns_list = ascending_columns_with_number + permanent_column_list
                print('@@Single row data final@@')
                print(final_columns_list)

                # Find new columns needing addition
                missing_columns = [col for col in final_columns_list if col not in df.columns]
                # Construct a DataFrame all None
                new_cols_df = pd.DataFrame({col: [None] * len(df) for col in missing_columns})
                # Merge df
                df = pd.concat([df, new_cols_df], axis=1)

                # Filter out target fields
                df_filter = df[final_columns_list].copy()
                # Rename column headers
                df_filter = df_filter.rename(columns=rename_columns_dict)
                # Concatenate to form final result
                final_df = pd.concat([final_df, df_filter])

        return final_df

    def run(self):
        # 1. Initialize paths
        # Storage path for file final structure and column correspondence
        struct_dir = self.struct_dir

        # Flatten result path
        flatten_res_dir = self.flatten_res_dir

        # Storage path
        main_dir = self.output_dir
        utils.create_dir(main_dir)

        directory_list = os.listdir(struct_dir)
        for dir in directory_list:

            # Initialize storage path
            save_dir = os.path.join(main_dir, dir)
            utils.create_dir(save_dir)

            # Initialize file structure
            struct_data_dir = os.path.join(struct_dir, dir)
            file_list = os.listdir(struct_data_dir)
            for file in file_list:
                print('##############')
                print(file)
                # if file != 'API 50CH assim.xlsx':
                #     continue

                # Given file structure
                struct_data_path = os.path.join(struct_data_dir, file)
                df_SD = pd.read_excel(struct_data_path, dtype=str, index_col=False)
                print(df_SD.columns)
                append_column, max_count, according_dict = self.get_struct_columns_dict(df_SD)
                print('%%%%%%')
                print(f"append_column (supplement columns): {append_column}")
                print(f"max_count (maximum count of duplicate columns): {max_count}")
                print(f"according_dict (rename reference dict): {according_dict}")

                # Original data file
                original_file = f"{dir}.tsv"
                original_data_path = os.path.join(flatten_res_dir, original_file)
                df_OD = utils.load_df(original_data_path)

                # print(struct_data_path)
                # print(original_data_path)

                df_final = self.build_final_df(
                    df=df_OD,
                    append_column=append_column,
                    max_count=max_count,
                    according_dict=according_dict
                )

                # Remove data rows where only file information columns not empty
                file_message_columns = ['source_file', 'bacdive_id']
                columns_list = df_final.columns.tolist()
                for column in file_message_columns:
                    while column in columns_list:
                        columns_list.remove(column)
                df_final = df_final.dropna(subset=columns_list, how='all')

                # Remove duplicate data
                df_final = df_final.drop_duplicates()

                save_name = file.replace('.xlsx', '.tsv')
                save_path = os.path.join(save_dir, save_name)
                if len(df_final) > 0:
                    utils.save_df(save_path, df_final)