"""
Editor: INK
Create Time: 2025/3/5 14:56
File Name: Microbewiki_3rd_clean.py
Function: 
"""
import os.path

import pandas as pd

import utils.my_df_function as utils


class MicrobeWikiSplitTable:

    @staticmethod
    def modify_data_1(df):
        df = df.fillna('NA_NO')
        return df

    def modify_data_2(self, df, save_dir):

        record_list = []
        record_dir = os.path.join(self.output_dir, 'Record')
        record_path = os.path.join(record_dir, 'record.tsv')
        utils.create_dir(record_dir)

        common_columns = [
            'species_name',
            'source_file',
            'references',
            'lishan_species_tax_id',
            'species_tax_id',
            'subspecies_name',
            'subspecies_tax_id',
            'lishan_subspecies_tax_id',
            'serotype_name',
            'serotype_tax_id',
            'lishan_serotype_tax_id',
            'lishan_csn_id',
            'current_scientific_name',
        ]  # Common columns

        # Initialize empty value columns
        for columns in common_columns:
            if columns not in df.columns:
                df[columns] = 'NA_NO'

        target_columns = [
            'application_to_biotechnology',
            'cell_structure_and_metabolism',
            'clinical',
            'current_research',
            'description_and_significance',
            'ecology_and_pathogenesis',
            'genome_structure',
            'interesting_feature',
        ]  # Special columns, need separately process into tables
        for column in target_columns:
            final_columns = common_columns + [column]
            if column in df.columns.tolist():
                df_split = df[final_columns].copy()
                df_split = df_split[df_split[column] != 'NA_NO']
            else:
                df_split = pd.DataFrame(columns=final_columns)

            save_name = f'{column}.tsv'
            save_path = os.path.join(save_dir, save_name)
            utils.save_df(save_path, df_split)
            d = {
                'file_name': save_name,
                'file_count': len(df_split)
            }
            record_list.append(d)

        df_record = pd.DataFrame(record_list)
        utils.save_df(record_path, df_record)

    def __init__(self, input_path, output_dir):
        self.input_path = input_path
        self.output_dir = output_dir

    def run(self):
        data_path = self.input_path
        df = utils.load_df(data_path)

        df = self.modify_data_1(df)

        save_dir = os.path.join(self.output_dir, 'Data')
        utils.create_dir(save_dir)
        self.modify_data_2(df, save_dir=save_dir)
