import json
import os
import re
import shutil
import pandas as pd
import utils.my_df_function as until


class SpeciesConsolidator:

    # [Prepare] Preprocessing: Build mapping between lishan_species_tax_id and file_name_id for subsequent aggregation by lishan_species_tax_id
    @staticmethod
    def create_taxonomy_hierarchy_map(all_strains_sop_data_dir, save_dir):
        # Get which file_name_id correspond to each lishan_species_tax_id
        dict_species_scan = {}
        file_list = os.listdir(all_strains_sop_data_dir)
        for species_strains_id in file_list:
            print(species_strains_id)
            temp_list = species_strains_id.split('_')
            lishan_species_tax_id = temp_list[0] + '_' + temp_list[1]
            if lishan_species_tax_id not in dict_species_scan:
                d = {lishan_species_tax_id: [species_strains_id]}
            else:
                old_list = dict_species_scan.get(lishan_species_tax_id)
                old_list.append(species_strains_id)
                new_list = old_list
                d = {lishan_species_tax_id: new_list}
            dict_species_scan.update(d)

        save_dir = save_dir + 'Prepare/'
        with open(save_dir + 'aggregation_basement_dict.json', 'w+') as f:
            json.dump(dict_species_scan, f, indent=4)

        return dict_species_scan

    # Merge relation tables when species_tax_id values are the same
    @staticmethod
    def consolidate_gene_records_across_strains(dir_strain_sop_res, target_file_name_list, save_dir, lishan_species):
        # Construct storage path
        until.create_dir(save_dir)

        # Entity table
        # Merge sequentially
        df_entity_all = pd.DataFrame()
        for species_strains_id in target_file_name_list:
            entity_file_name = f'{species_strains_id}_gene_ncbi_bio.tsv'
            data_path = os.path.join(dir_strain_sop_res, species_strains_id, 'stage9_final_standardization', 'Data', entity_file_name)

            df = until.load_df(data_path)
            df_entity_all = pd.concat([df_entity_all, df])

        # Initialize storage path
        entity_save_file_name = '1st_' + lishan_species + '_gene_v0.tsv'
        entity_save_path = save_dir + entity_save_file_name
        until.save_df(entity_save_path, df_entity_all)

        return df_entity_all

    # Filter duplicate data
    @staticmethod
    def filter_duplicate_gene_annotations(father_save_dir, lishan_species):
        # Remove duplicates across all fields
        entity_file_name = '1st_' + lishan_species + '_gene_v0.tsv'
        relation_file_name = '1st_' + lishan_species + '_gene2protein_v0.tsv'
        entity_path = father_save_dir + 'stage1_consolidate_gene_records_across_strains/' + entity_file_name
        relation_path = father_save_dir + 'stage1_consolidate_gene_records_across_strains/' + relation_file_name

        # Construct storage path
        save_dir = father_save_dir + 'stage2_filter_duplicate_gene_annotations/'
        until.create_dir(save_dir)

        # Record count
        dealing_record_path = save_dir + 'Status of data processing.txt'
        with open(dealing_record_path, 'w+'):
            pass

        # Duplicate condition record
        duplicated_record_path = save_dir + 'duplicated_circumstance.txt'
        with open(duplicated_record_path, 'w+'):
            pass

        # Load data
        df_entity = until.load_df(entity_path)
        df_relation = until.load_df(relation_path)

        # Remove duplicates across all fields
        df_entity = df_entity.drop_duplicates()
        df_relation = df_relation.drop_duplicates()

        # Store duplicate data
        file_entity_dup = lishan_species + '_gene_biocyc_id_same_otherdiff_gene.tsv'
        path_entity_dup = save_dir + file_entity_dup
        file_relation_dup = lishan_species + '_gene_biocyc_id_same_otherdiff_gene2protein.tsv'
        path_relation_dup = save_dir + file_relation_dup

        # Get data where NCBI_gene_id and gene_biocyc_id are both identical
        dup_columns = ['NCBI_gene_id', 'gene_biocyc_id']
        df_entity_dup = df_entity[df_entity.duplicated(subset=dup_columns, keep=False)]
        if len(df_entity_dup) > 0:
            with open(duplicated_record_path, 'a+') as f_dup:
                f_dup.write(entity_file_name + '→【Same NCBI_gene_id and gene_biocyc_id】'
                                               '→contains eligible duplicate data!\n')
            until.save_df(path_entity_dup, df_entity_dup)
        else:
            with open(duplicated_record_path, 'a+') as f_dup:
                f_dup.write(entity_file_name + '→【Same NCBI_gene_id and gene_biocyc_id】→'
                                               'do not contain eligible duplicate data!\n')

        # Handle cases where NCBI_gene_id are same but gene_biocyc_id are different
        def deal_dup_ncbi_gene_id(df):
            """
            :param df:
            :return:
                # The final result consists of three parts
                # df_na, data where NCBI_gene_id is 'NA_NO'
                df_dup_not_na
                df_not_dup_not_na
            """
            df_na = df[df['NCBI_gene_id'] == 'NA_NO'].copy()
            df_not_na = df[df['NCBI_gene_id'] != 'NA_NO'].copy()

            # Step 1: Distinguish rows with duplicate NCBI_gene_id from rows with unique NCBI_gene_id
            is_duplicated_gene = df_not_na['NCBI_gene_id'].duplicated(keep=False)
            df_not_dup_not_na = df_not_na[-is_duplicated_gene]

            # Step 2: Exclude rows where gene_biocyc_id is 'NA_NO'
            is_not_na_no = df_not_na['gene_biocyc_id'] != 'NA_NO'

            # Step 3: Group by NCBI_gene_id to find rows with different gene_biocyc_id
            df_filtered = df_not_na[is_duplicated_gene & is_not_na_no]

            # Ensure gene_biocyc_id are not identical
            df_still_dup = df_filtered[df_filtered.duplicated(subset=['NCBI_gene_id', 'gene_biocyc_id'], keep=False)]

            # The final result consists of three parts
            df_filtered = pd.concat([df_na, df_not_dup_not_na, df_filtered])

            return df_filtered, df_still_dup

        file_entity_diff = lishan_species + '_gene_biocyc_id_different_gene.tsv'
        path_entity_diff = save_dir + file_entity_diff

        df_entity, df_entity_diff = deal_dup_ncbi_gene_id(df_entity)

        if len(df_entity_diff) > 0:
            with open(duplicated_record_path, 'a+') as f_dup:
                f_dup.write(entity_file_name + '→【Same NCBI_gene_id, Different gene_biocyc_id】'
                                               '→contains eligible duplicate data!\n')
            until.save_df(path_entity_diff, df_entity_diff)

        else:
            with open(duplicated_record_path, 'a+') as f_dup:
                f_dup.write(entity_file_name + '→【Same NCBI_gene_id, Different gene_biocyc_id】'
                                               '→do not contains eligible duplicate data!\n')

        # Store final result of this function
        final_entity_file = '2nd_' + lishan_species + '_gene_v0.tsv'
        path_entity_final = save_dir + final_entity_file
        until.save_df(path_entity_final, df_entity)

    # Unify column headers for all data
    @staticmethod
    def normalize_data_schema(main_dir):
        # Initialize column header scanning result
        total_columns_summary = []

        # Scan and summarize column headers sequentially
        data_dir = main_dir + 'Data/'
        lishan_species_list = os.listdir(data_dir)
        for lishan_species_tax_id in lishan_species_list:
            data_path = data_dir + f'{lishan_species_tax_id}/stage1_consolidate_gene_records_across_strains/1st_{lishan_species_tax_id}_gene_v0.tsv'
            df = until.load_df(data_path)
            columns_list = df.columns.to_list()

            for column in columns_list:
                if column not in total_columns_summary:
                    total_columns_summary.append(column)

        # Unify column headers sequentially
        for lishan_species_tax_id in lishan_species_list:

            # Processing condition record
            file_count_list = []

            # Load data
            data_path = data_dir + f'{lishan_species_tax_id}/stage1_consolidate_gene_records_across_strains/1st_{lishan_species_tax_id}_gene_v0.tsv'
            df = until.load_df(data_path)
            d = {
                'file_name': f'1st_{lishan_species_tax_id}_gene_v0.tsv',
                'file_count': len(df)
            }
            file_count_list.append(d)

            # Get missing columns
            missing_columns = [col for col in total_columns_summary if col not in df.columns]
            if missing_columns:
                defaults = pd.DataFrame('NA_NO', index=df.index, columns=missing_columns)
                df = pd.concat([df, defaults], axis=1)

            # Store column unification result
            save_dir = main_dir + f'Data/{lishan_species_tax_id}/stage3_normalize_data_schema/'
            until.create_dir(save_dir)
            save_name = f'3rd_{lishan_species_tax_id}_gene_v0.tsv'
            until.save_df(save_dir + save_name, df)

            d = {
                'file_name': save_name,
                'file_count': len(df)
            }
            file_count_list.append(d)

            # Store processing record
            df_record = pd.DataFrame(file_count_list)
            until.save_df(save_dir + 'record.tsv', df_record)

        save_dir_columns_dir = main_dir + 'Record/'
        save_name = 'columns_summary.txt'
        for column in total_columns_summary:
            with open(save_dir_columns_dir + save_name, 'w+') as f:
                f.write(column)
                f.write('\n')

    # Store a copy for submission
    @staticmethod
    def generate_submission_dataset(main_dir):
        # Store final submission result
        final_submit_main_dir = main_dir + f'Submission/'
        until.create_dir(final_submit_main_dir)

        final_submit_data_dir = final_submit_main_dir + '/Data/'
        until.create_dir(final_submit_data_dir)

        record_dict_list = []

        # Copy column unification result as final submission result
        data_dir = main_dir + 'Data/'
        lishan_species_list = os.listdir(data_dir)
        for lishan_species_tax_id in lishan_species_list:
            data_path = data_dir + f'{lishan_species_tax_id}/stage3_normalize_data_schema/3rd_{lishan_species_tax_id}_gene_v0.tsv'
            df = until.load_df(data_path)
            d = {
                'file_name': f'3rd_{lishan_species_tax_id}_gene_v0.tsv',
                'file_count': len(df)
            }
            record_dict_list.append(d)

            save_name = f'3rd_{lishan_species_tax_id}_gene_v0.tsv'
            until.save_df(final_submit_data_dir + save_name, df)

        df_record = pd.DataFrame(record_dict_list)
        until.save_df(final_submit_main_dir + 'record.tsv', df_record)

    # Classify data by biological level
    @staticmethod
    def categorize_by_biological_level(main_dir, ref_path, category_save_dir):
        data_dir = main_dir + 'Data/'
        lishan_species_list = os.listdir(data_dir)
        for lishan_species_tax_id in lishan_species_list:

            # Load original data to be split
            original_data_dir = data_dir + f'{lishan_species_tax_id}/stage3_normalize_data_schema/'
            original_data_path = original_data_dir + '3rd_' + lishan_species_tax_id + '_gene_v0.tsv'
            df_original = until.load_df(original_data_path)

            # Process ref table
            df_ref = until.load_df(ref_path)

            # Define columns to check
            category_columns = [
                'lishan_species_tax_id',
                'lishan_subspecies_tax_id',
                'lishan_serotype_tax_id',
                'lishan_strains_tax_id',
                'lishan_substrains_tax_id'
            ]

            # Add a column to df_original for storing classification
            df_original['category'] = None

            # Iterate through each row of df_original
            for index, row in df_original.iterrows():
                # Find rows in df_ref with same lishan_species_tax_id
                matching_ref = df_ref[df_ref['lishan_species_tax_id'] == row['lishan_species_tax_id']]

                # Check columns matching lishan_csn_id in matching_ref
                for col in category_columns:
                    if row['lishan_csn_id'] in matching_ref[col].values:
                        df_original.at[index, 'category'] = col
                        break  # Stop further checking once a match is found
            # until.save_df('test.tsv', df_original)

            # Group by different categories
            save_directory_dict = {
                'lishan_species_tax_id': 'species',
                'lishan_subspecies_tax_id': 'subspecies',
                'lishan_serotype_tax_id': 'serotype',
                'lishan_strains_tax_id': 'strains',
                'lishan_substrains_tax_id': 'substrains'
            }
            for category in category_columns:
                # Get data of current category
                category_data = df_original[df_original['category'] == category]

                # If data exists, save to corresponding TSV file
                if not category_data.empty:

                    # Construct storage path
                    directory_name = save_directory_dict.get(category)
                    save_dir = category_save_dir + directory_name + '/'
                    until.create_dir(save_dir)

                    # Remove discriminant column
                    drop_column = ['category']
                    category_data = category_data.drop(columns=drop_column)

                    # Store
                    if directory_name == 'strains_tax_id':
                        directory_name = 'strains'
                    file_name = f"{lishan_species_tax_id}_{directory_name}_gene_v0.tsv"
                    save_path = save_dir + file_name
                    until.save_df(save_path, category_data)  # Save as Tsv file

                    # Store total data count
                    record_path = save_dir + 'Status of data processing.txt'

        print("Data has been saved to corresponding files by category.")

    def __init__(self, gene_strains_dir, output_dir, ref_path):
        self.gene_strains_dir = gene_strains_dir
        self.output_dir = output_dir
        self.ref_path = ref_path
        self._init_directory()

    def _init_directory(self):
        until.create_dir(self.output_dir + 'Data/')
        until.create_dir(self.output_dir + 'Record/')
        until.create_dir(self.output_dir + 'Source/')
        until.create_dir(self.output_dir + 'Prepare/')

    # Start entire processing flow
    def run(self):

        # Path to ref table used during processing
        ref_path = self.ref_path
        # Main storage path for all processing results
        save_dir_main = self.output_dir

        # Initialize data folders for storage by biological definition classification
        category_save_dir = f'{save_dir_main}/Category/'
        if os.path.exists(category_save_dir):
            shutil.rmtree(category_save_dir)
        until.create_dir(category_save_dir)

        # Store processing result Record
        record_dir = f'{save_dir_main}/Record/'
        until.create_dir(record_dir)
        record_path = record_dir + 'Gene_of_NCBI_Biocyc_Species.json'
        record_dict = until.record_json_get(record_path)

        # Initialize error record storage
        error_path = f'{record_dir}/gene_species_error.txt'
        with open(error_path, 'w+'):
            pass

        # [Prepare] Get which file_name_id correspond to each lishan_species_tax_id
        dict_species_scan = self.create_taxonomy_hierarchy_map(
            all_strains_sop_data_dir=self.gene_strains_dir,
            save_dir=save_dir_main
        )

        # Perform merging processing sequentially
        for lishan_species_tax_id in dict_species_scan:
            print('#################')
            print(lishan_species_tax_id)
            try:
                target_file_name_list = dict_species_scan.get(lishan_species_tax_id)

                # Construct storage path for processing results
                organism_dir = save_dir_main + f'/Data/{lishan_species_tax_id}/'
                until.create_dir(organism_dir)

                # Skip previously successfully processed data
                # if lishan_species_tax_id in record_dict:
                #     deal_status = record_dict.get(lishan_species_tax_id)
                #     if deal_status == 'success':
                #         continue

                print('Merge strain-level data with the same species ！')
                # Merge Gene entity tables and relation tables when species_tax_id are same
                save_dir = organism_dir + 'stage1_consolidate_gene_records_across_strains/'
                self.consolidate_gene_records_across_strains(
                    dir_strain_sop_res=self.gene_strains_dir,
                    target_file_name_list=target_file_name_list,
                    save_dir=save_dir,
                    lishan_species=lishan_species_tax_id
                )
                print('#############')

                # Filter out duplicate values
                print('Filter out duplicate values')
                self.filter_duplicate_gene_annotations(
                    father_save_dir=organism_dir,
                    lishan_species=lishan_species_tax_id,
                )
                print('#############')

                record_d = {lishan_species_tax_id: 'success'}

            except Exception as e:
                record_d = {lishan_species_tax_id: 'fail'}
                with open(error_path, 'a+') as f:
                    f.write(lishan_species_tax_id)
                    f.write('\t' + str(e) + '\n')

            record_dict.update(record_d)
            with open(record_path, 'w+') as f:
                json.dump(record_dict, f, indent=4)

        # Unify column headers of processing results
        self.normalize_data_schema(main_dir=save_dir_main)

        # Get submission data
        self.generate_submission_dataset(main_dir=save_dir_main)

        # Classify data by biological level
        print('Classify data by biological level')
        self.categorize_by_biological_level(
            main_dir=save_dir_main,
            ref_path=ref_path,
            category_save_dir=category_save_dir
        )


if __name__ == '__main__':
    test = SpeciesConsolidator()
    test.run()
