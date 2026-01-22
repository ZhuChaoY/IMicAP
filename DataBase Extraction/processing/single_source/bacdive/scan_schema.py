"""
Editor: INK
Create Time: 2025/3/24 14:18
File Name: scan_schema.py
Function: 
Count file quantity and column name completeness of BacDive data processing results
"""
import os.path

import pandas as pd
import utils.my_df_function as utils


class BacDiveSchemaScanner:
    def __init__(self, flatten_dir, processed_dir, struct_dir, out_dir):
        self.file_count_list = []
        self.columns_compare_res = []
        self.now_time = utils.get_now_time()

        self.flatten_dir = flatten_dir
        self.processed_dir = processed_dir
        self.struct_dir = struct_dir
        self.save_dir = out_dir

    def run(self):
        """
        Main scanning method
        :param flatten_dir: Flatten result directory
        :param processed_dir: Processed result directory
        :param struct_dir: Structure definition directory
        :param save_dir: Result save directory
        """
        # Create save directory
        utils.create_dir(self.save_dir)

        # Scan flatten results
        self._scan_flatten_data(self.flatten_dir)

        # Scan processed results and check column names
        self._scan_processed_data(self.processed_dir, self.struct_dir)

        # Save results
        self._save_results(self.save_dir)

    def _scan_flatten_data(self, flatten_dir):
        """Scan flattened data files"""
        path_dict_flatten = utils.get_file_list(flatten_dir, {})
        for file in path_dict_flatten:
            data_path = path_dict_flatten.get(file)
            df = utils.load_df(data_path)
            self.file_count_list.append({
                'file_name': file,
                'file_count': len(df),
                'source': 'Json_flatten'
            })

    def _scan_processed_data(self, processed_dir, struct_dir):
        """Scan processed data and check column names"""
        path_dict_processed = utils.get_file_list(processed_dir, {})
        columns_condition_file_dict = utils.get_file_list(struct_dir, {})

        for file in path_dict_processed:
            data_path = path_dict_processed.get(file)
            df = utils.load_df(data_path)

            # Record file count
            self.file_count_list.append({
                'file_name': file,
                'file_count': len(df),
                'source': f'BacDive_clean'
            })

            # Check column names
            self._check_columns(file, df, columns_condition_file_dict)

    def _check_columns(self, file, df, columns_condition_file_dict):
        """Check if column names meet expectations"""
        file_struct = file.replace(f'.tsv', '.xlsx')
        struct_file_path = columns_condition_file_dict.get(file_struct)

        print('#########')
        print(struct_file_path)
        if not struct_file_path:
            print(f"Warning: No structure file found for {file}")
            return

        df_struct = pd.read_excel(struct_file_path, dtype=str, index_col=False)
        SC_columns_list = df_struct['SC'].tolist()

        for sc_col in SC_columns_list:
            is_contains = sc_col in df.columns
            self.columns_compare_res.append({
                'file': file,
                'column_name': sc_col,
                'is_contains': is_contains
            })

            if not is_contains:
                print(f"Missing column: {sc_col} in {file}")

    def _save_results(self, save_dir):
        """Save scan results"""
        # Save file count results
        path_count_res = os.path.join(save_dir, 'file_count_summary.tsv')
        df_count = pd.DataFrame(self.file_count_list)
        utils.save_df(path_count_res, df_count)

        # Save column name check results
        path_columns_res = os.path.join(save_dir, 'file_columns_summary.tsv')
        df_columns = pd.DataFrame(self.columns_compare_res)
        utils.save_df(path_columns_res, df_columns)
