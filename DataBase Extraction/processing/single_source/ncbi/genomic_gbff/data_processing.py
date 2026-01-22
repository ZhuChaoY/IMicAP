"""
Editor: INK
Create Time: 2024/11/25 9:40
File Name: data_processing.py
Function:
    Filter genomic_gbff parsing results
"""
import os
import re

import numpy as np
import pandas as pd

import utils.my_df_function as utils
from utils import common_data_modify


class GbffProcessor:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir

    @staticmethod
    def basic_data_clean(data_dir, directory_name):
        original_file = f'ncbi_gbff_basic_data.tsv'
        data_path = os.path.join(data_dir, original_file)
        df = utils.load_df(data_path)

        # Reconstruct sequence_version
        df['sequence_version'] = df['record_name'] + '.' + df['sequence_version']

        # Process Features Annotated multi-values separated by ;
        def deal_features_annotated(data):
            data_list = data.split('; ')
            new_data = utils.change_list_to_special_data(data_list)
            return new_data

        if 'Features Annotated' in df.columns:
            df['Features Annotated'] = df['Features Annotated'].apply(lambda x: deal_features_annotated(x))

        # Process extra ',' symbols in numbers
        def deal_waste_signal(data):
            data = data.replace(',', '')
            return data

        columns_list = [
            'Genes (total)',
            'CDSs (total)',
            'Genes (coding)',
            'CDSs (with protein)'
        ]
        columns_list = utils.conclude_prepare_columns(columns_list, df)
        for column in columns_list:
            df[column] = df[column].apply(lambda x: deal_waste_signal(x))

        # Add Assembly column
        temp_list = directory_name.split('_')
        assembly = temp_list[0] + '_' + temp_list[1]
        df['assembly'] = assembly

        # Fill empty values
        df = df.fillna('NA_NO')

        # Unify column headers
        df = common_data_modify.change_columns_to_same_format(df, [])

        sequence_version_dict = df.set_index('record_name')['sequence_version'].to_dict()

        return df, sequence_version_dict

    @staticmethod
    def ref_data_clean(data_dir, directory_name, sequence_version_dict):
        original_file = f'ncbi_gbff_reference.tsv'
        data_path = os.path.join(data_dir, original_file)
        df = utils.load_df(data_path)

        df = df.fillna('NA_NO')

        # Process location
        def deal_location(data):
            if data == 'NA_NO':
                return data

            data = data.replace('[', '')
            data = data.replace(']', '')

            data_list = data.split(':')
            start_num = str(int(data_list[0]) + 1)
            end_num = data_list[1]

            new_data = start_num + '...' + end_num
            return new_data

        df['location'] = df['location'].apply(lambda x: deal_location(x))

        # Add Assembly column
        temp_list = directory_name.split('_')
        assembly = temp_list[0] + '_' + temp_list[1]
        df['assembly'] = assembly

        # Add seq version
        def build_seq(record_name):
            if record_name in sequence_version_dict:
                sequence_version = sequence_version_dict.get(record_name)
                return sequence_version
            else:
                return 'NA_NO'

        df['sequence_version'] = df['record_name'].apply(lambda x: build_seq(x))

        # Add primary key
        df['temp_index'] = (df.index + 1).astype(str)
        df['reference'] = df['assembly'] + '_' + df['temp_index']
        drop_column = ['temp_index']
        df = df.drop(columns=drop_column)

        # Fill empty values
        df = df.fillna('NA_NO')

        # Unify column headers
        df = common_data_modify.change_columns_to_same_format(df, [])

        return df

    def feature_data_clean(self, data_dir, directory_name, sequence_version_dict):
        original_file = f'ncbi_gbff_feature.tsv'
        data_path = os.path.join(data_dir, original_file)
        df = utils.load_df(data_path)

        # First get location subtable
        df_location = self._feature_data_clean_exact_location(df)

        drop_column = [
            'id',
            'ref',
            'ref_db',
            'organism',
            'strain',
            'location',
            'strand',
        ]
        drop_column = utils.conclude_prepare_columns(drop_column, df)
        df = df.drop(columns=drop_column)

        # Modify other rows' data according to source row data, simultaneously add sequence_version
        def modify_by_source(df):

            # Filter out type source data, and delete in original data
            df_source = df[df['type'] == 'source']
            if len(df_source) == 0:
                return df
            df = df[df['type'] != 'source']

            # Specify columns to be used as values
            columns_to_include = ['mol_type', 'isolation_source', 'host', 'type_material', 'chromosome']
            columns_to_include = utils.conclude_prepare_columns(columns_to_include, df)

            # Build source data dictionary
            source_data_dict = {}
            for index, row in df_source.iterrows():
                record_name = row['record_name']
                value_dict = {col: row[col] for col in columns_to_include}
                source_data_dict[record_name] = value_dict
            print(source_data_dict)

            # Modify data according to source data dictionary
            df_modify_res = pd.DataFrame()
            for record_name in source_data_dict:
                modify_data_dict = source_data_dict.get(record_name)
                df_filter = df[df['record_name'] == record_name].copy()

                # Unify modify data values
                for col in modify_data_dict:
                    source_value = modify_data_dict.get(col)
                    df_filter[col] = source_value

                df_modify_res = pd.concat([df_modify_res, df_filter])
            return df_modify_res

        df = modify_by_source(df)

        # Add seq version
        def build_seq(record_name):
            if record_name in sequence_version_dict:
                sequence_version = sequence_version_dict.get(record_name)
                return sequence_version
            else:
                return 'NA_NO'

        df['sequence_version'] = df['record_name'].apply(lambda x: build_seq(x))

        # Add Assembly column
        temp_list = directory_name.split('_')
        assembly = temp_list[0] + '_' + temp_list[1]
        df['assembly'] = assembly

        # Modify column names
        change_dict = {
            'type': 'feature_type'
        }
        df = df.rename(columns=change_dict)

        # Fill empty values
        df = df.fillna('NA_NO')
        # Unify column headers
        df = common_data_modify.change_columns_to_same_format(df, [])

        # Split tables, gene
        df_gene = df[df['feature_type'] == 'gene']
        df = df[df['feature_type'] != 'gene']

        df_cds = df[df['feature_type'] == 'CDS']
        df = df[df['feature_type'] != 'CDS']

        RNA_list = ['rRNA',
                    'tRNA',
                    'ncRNA',
                    'tmRNA',
                    'mRNA'
                    ]
        df_RNA = df[df['feature_type'].isin(RNA_list)]

        df_other = df[-df['feature_type'].isin(RNA_list)]

        return df_location, df_gene, df_cds, df_RNA, df_other

    @staticmethod
    def _feature_data_clean_exact_location(df):
        location_columns = ['location',
                            'strand',
                            'type',
                            'locus_tag']
        df_location = df[location_columns].copy()
        df_location = df_location[df_location['type'] != 'source']

        def build_recombination(df_location):
            # Condition judgment
            has_brace = df_location["location"].str.contains("{", na=False)  # Whether contains {
            starts_with_join = df_location["location"].str.startswith("join")  # Whether starts with join

            # Use np.select for multi-condition assignment
            conditions = [
                (has_brace & starts_with_join),  # yes
                (has_brace & ~starts_with_join),  # uncertain
                (~has_brace),  # no
            ]
            choices = ["yes", "uncertain", "no"]

            df_location["recombination"] = np.select(conditions, choices, default="no")
            return df_location

        df_location = build_recombination(df_location)

        # Extract data in location with "[xxx](y)" as pattern symbol for subsequent explode use
        def match_location_pattern(data):
            pattern = r'\[[^\]]+\]\([^)]+\)'
            matches = re.findall(pattern, data)
            return matches

        df_location['location'] = df_location['location'].apply(lambda x: match_location_pattern(x))
        df_location = df_location.explode(column='location')

        # Split location, get start, end, strand
        def split_location(location_str):
            start = 'NA_NO'
            end = 'NA_NO'
            strand = 'NA_NO'

            # Get start and end
            Mapping = r'\[<?(\d+):>?(\d+)\]'
            match = re.match(Mapping, location_str)
            if match:
                start = str(int(match.group(1)) + 1)
                end = str(int(match.group(2)))
            # Get strand
            strand_matches = re.findall(r'\(([^)]*)\)', location_str)
            if strand_matches:
                strand = strand_matches[0]  # Take first matching bracket content
            return pd.Series([start, end, strand])

        df_location[['start', 'end', 'strand']] = df_location['location'].apply(split_location)

        df_location = df_location.drop('location', axis=1)
        rename_columns = {
            'type': 'feature_type'
        }
        df_location = df_location.rename(columns=rename_columns)

        return df_location

    @staticmethod
    def save_location_data(df, save_dir_all, directory_name):

        # Store location data
        save_dir = os.path.join(save_dir_all, 'Data', 'location_data')
        utils.create_dir(save_dir)
        file_name = f'{directory_name}_subtable_location.tsv'
        save_path = os.path.join(save_dir, file_name)
        utils.save_df(save_path, df)

        # Split store location data
        # Split tables, gene
        df_gene = df[df['feature_type'] == 'gene']
        df = df[df['feature_type'] != 'gene']

        df_cds = df[df['feature_type'] == 'CDS']
        df = df[df['feature_type'] != 'CDS']

        RNA_list = ['rRNA',
                    'tRNA',
                    'ncRNA',
                    'tmRNA',
                    'mRNA'
                    ]
        df_RNA = df[df['feature_type'].isin(RNA_list)]

        df_other = df[~df['feature_type'].isin(RNA_list)]

        # Store split results separately
        save_dir = os.path.join(save_dir_all, 'Data', 'gene_subtable_location_data')
        utils.create_dir(save_dir)
        file_name = f'{directory_name}_Gene_subtable_location_data.tsv'
        save_path = os.path.join(save_dir, file_name)
        utils.save_df(save_path, df_gene)

        save_dir = os.path.join(save_dir_all, 'Data', 'CDS_subtable_location_data')
        utils.create_dir(save_dir)
        file_name = f'{directory_name}_CDS_subtable_location_data.tsv'
        save_path = os.path.join(save_dir, file_name)
        utils.save_df(save_path, df_cds)

        save_dir = os.path.join(save_dir_all, 'Data', 'RNA_subtable_location_data')
        utils.create_dir(save_dir)
        file_name = f'{directory_name}_RNA_subtable_location_data.tsv'
        save_path = os.path.join(save_dir, file_name)
        utils.save_df(save_path, df_RNA)

        save_dir = os.path.join(save_dir_all, 'Data', 'other_subtable_location_data')
        utils.create_dir(save_dir)
        file_name = f'{directory_name}_other_subtable_location_data.tsv'
        save_path = os.path.join(save_dir, file_name)
        utils.save_df(save_path, df_other)

    def run(self):
        data_dir = self.input_dir

        save_dir_all = self.output_dir
        utils.create_dir(save_dir_all)

        list_directory_name = os.listdir(data_dir)

        for directory_name in list_directory_name:
            detail_data_dir = os.path.join(data_dir, directory_name)

            df, sequence_version_dict = self.basic_data_clean(
                data_dir=detail_data_dir,
                directory_name=directory_name,
            )
            save_dir = os.path.join(save_dir_all, 'Data', 'genome_data')
            utils.create_dir(save_dir)
            file_name = f'{directory_name}_genome_data.tsv'
            save_path = os.path.join(save_dir, file_name)
            utils.save_df(save_path, df)

            # Process ncbi_gbff_reference_data
            df = self.ref_data_clean(
                data_dir=detail_data_dir,
                directory_name=directory_name,
                sequence_version_dict=sequence_version_dict,
            )
            save_dir = os.path.join(save_dir_all, 'Data', 'reference_data')
            utils.create_dir(save_dir)
            file_name = f'{directory_name}_reference_data.tsv'
            save_path = os.path.join(save_dir, file_name)
            utils.save_df(save_path, df)

            # Process ncbi_gbff_feature
            df_location, df_gene, df_cds, df_RNA, df_other = self.feature_data_clean(
                data_dir=detail_data_dir,
                directory_name=directory_name,
                sequence_version_dict=sequence_version_dict,
            )

            # Store location data
            self.save_location_data(
                df=df_location,
                save_dir_all=save_dir_all,
                directory_name=directory_name,
            )

            # Store feature data
            save_dir = os.path.join(save_dir_all, 'Data', 'gene_data')
            utils.create_dir(save_dir)
            file_name = f'{directory_name}_Gene_data.tsv'
            save_path = os.path.join(save_dir, file_name)
            utils.save_df(save_path, df_gene)

            save_dir = os.path.join(save_dir_all, 'Data', 'CDS_data')
            utils.create_dir(save_dir)
            file_name = f'{directory_name}_CDS_data.tsv'
            save_path = os.path.join(save_dir, file_name)
            utils.save_df(save_path, df_cds)

            save_dir = os.path.join(save_dir_all, 'Data', 'RNA_data')
            utils.create_dir(save_dir)
            file_name = f'{directory_name}_RNA_data.tsv'
            save_path = os.path.join(save_dir, file_name)
            utils.save_df(save_path, df_RNA)

            save_dir = os.path.join(save_dir_all, 'Data', 'other_data')
            utils.create_dir(save_dir)
            file_name = f'{directory_name}_other_data.tsv'
            save_path = os.path.join(save_dir, file_name)
            utils.save_df(save_path, df_other)
