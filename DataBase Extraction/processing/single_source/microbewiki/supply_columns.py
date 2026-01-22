"""
Editor: INK
Create Time: 2025/2/28 10:13
File Name: supply_columns.py
Function: 
"""
import os.path

import pandas as pd
import utils.my_df_function as utils


class MicrobeWikiSupplyColumns:
    def __init__(self, ref_path, csn_path, input_path, output_dir, save_name):
        self.ref_path = ref_path
        self.csn_path = csn_path
        self.input_path = input_path
        self.out_dir = output_dir
        self.save_name = save_name
    @staticmethod
    def append_ref(df, df_ref):
        ref_target_columns = [
            'species',
            'species_tax_id',
            'lishan_species_tax_id'
        ]
        df_ref_copy = df_ref[ref_target_columns].copy()
        df_ref_copy = df_ref_copy.drop_duplicates()

        final_column = df.columns.tolist() + ['species_tax_id', 'lishan_species_tax_id']

        df_merge = pd.merge(
            left=df,
            right=df_ref_copy,
            left_on='species_name',
            right_on='species',
            how='left'
        )
        df_merge = df_merge[final_column]
        return df_merge

    @staticmethod
    def append_csn(df, df_csn):
        csn_column = [
            'current_scientific_name',
            'lishan_csn_id'
        ]
        df_csn_copy = df_csn[csn_column].copy()

        df_merge = pd.merge(
            left=df,
            right=df_csn_copy,
            left_on='species_name',
            right_on='current_scientific_name',
            how='left'
        )

        final_column = df.columns.tolist() + ['lishan_csn_id', 'current_scientific_name']
        df_merge = df_merge[final_column]

        return df_merge

    @staticmethod
    def del_useless_str(df):
        for col in df.columns:
            df[col] = df[col].str.replace('\t', '', regex=False)

        df = df.fillna('NA_NO')

        return df

    def run(self):
        ref_path = self.ref_path
        df_ref = utils.load_df(ref_path)

        csn_path = self.csn_path
        df_csn = utils.load_df(csn_path)

        data_path = self.input_path
        df = utils.load_df(data_path)

        df = self.append_ref(df, df_ref=df_ref)
        df = self.append_csn(df, df_csn=df_csn)
        df = self.del_useless_str(df)

        save_dir = self.out_dir
        utils.create_dir(save_dir)
        utils.save_df(os.path.join(save_dir, self.save_name), df)
