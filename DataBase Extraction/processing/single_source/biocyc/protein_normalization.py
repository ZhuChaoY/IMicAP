import ast
import json
import os
import shutil
import numpy as np
import pandas as pd
import utils.my_df_function as utils


def start_combine_id(df):
    def combine_process(enzyme_id, unique_id):
        if pd.isnull(enzyme_id):
            return unique_id

        if pd.isnull(unique_id):
            return enzyme_id

        if enzyme_id == unique_id:
            return enzyme_id
        else:
            print('Problem occurred!')
            print(enzyme_id)
            print(unique_id)
            return enzyme_id

    df['Protein_BioCyc_ID'] = df.apply(lambda x: combine_process(x['enzyme_ID'], x['UNIQUE-ID']), axis=1)

    drop_column = ['UNIQUE-ID', 'enzyme_ID']
    df = df.drop(columns=drop_column)

    return df


def merge_process(df_link, df_dat):
    df_merge = pd.merge(df_link, df_dat,
                        left_on='enzyme_ID', right_on='UNIQUE-ID',
                        how='outer')

    return df_merge


# Split synonymes by comma, generate multi-value list
def split_synonymes(synonymes_data):
    if pd.isnull(synonymes_data):
        return synonymes_data

    list_synonymes = synonymes_data.split(', ')
    return list_synonymes


# Convert SYNONYMS to its true type
def transform_back_to_truth_type(SYNONYMS_data):
    if pd.isnull(SYNONYMS_data):
        return SYNONYMS_data

    try:
        truth_SYNONYMS = ast.literal_eval(SYNONYMS_data)
        if type(truth_SYNONYMS) == list:
            return truth_SYNONYMS
        else:
            return SYNONYMS_data
    except:
        return SYNONYMS_data


def start_combine_synonyms(df):
    # For handling situation where SYNONYMS or synonymes does not exist, ensure subsequent merge process normal
    target_column = ['synonymes', 'SYNONYMS']
    column_list = df.columns.tolist()
    for column in target_column:
        if column not in column_list:
            df[column] = np.nan

    # Handle situation where both synonymes and SYNONYMS exist
    # Convert data to original type
    df['SYNONYMS'] = df['SYNONYMS'].apply(lambda x: transform_back_to_truth_type(x))
    df['synonymes'] = df['synonymes'].apply(lambda x: split_synonymes(x))

    # Specific merge process
    def combine_process(synonymes, SYNONYMS):

        # Exclude special situations during merge process
        if type(synonymes) != list and pd.isnull(synonymes):
            final_res = SYNONYMS
        elif type(SYNONYMS) != list and pd.isnull(SYNONYMS):
            final_res = synonymes
        else:
            if type(synonymes) == list:
                list_synonyms_gene = synonymes
            else:
                list_synonyms_gene = [synonymes]

            if type(SYNONYMS) == list:
                list_synonyms_rna = SYNONYMS
            else:
                list_synonyms_rna = [SYNONYMS]

            final_res = list(set(list_synonyms_rna + list_synonyms_gene))

        # If final result empty no need to compare
        if final_res == np.nan:
            return final_res

        # If single element str, convert to list for convenient comparison
        if type(final_res) == list:
            list_final_res = final_res
        else:
            list_final_res = [final_res]

        final_str = utils.change_list_to_special_data(list_final_res)
        return final_str

    df['Synonyms'] = df.apply(
        lambda x: combine_process(x['synonymes'], x['SYNONYMS']), axis=1
    )

    # Delete column names
    drop_column = ['SYNONYMS', 'synonymes']
    df = df.drop(columns=drop_column)

    return df


def filer_dup_COMMON_NAME_ecocyc_id(df):
    # Since need to get COMMON_NAME case-insensitively, first convert COMMON_NAME to all lowercase
    df['conclude_COMMON_NAME'] = df['COMMON-NAME'].str.lower()

    # Filter out where ecocyc_id not empty, and same, COMMON_NAME also same
    df_notnull_eco_id = df[df['ecocyc_id'].notnull()]

    # Data meeting duplicate condition
    df_dup = df_notnull_eco_id[df_notnull_eco_id.duplicated(subset=['ecocyc_id', 'conclude_COMMON_NAME'], keep=False)]
    # print(df_dup)
    # Duplicate data's Protein_BioCyc_ID
    dup_id_list = df_dup['Protein_BioCyc_ID'].tolist()

    # Data not meeting duplicate condition
    df_not_dup = df[-df['Protein_BioCyc_ID'].isin(dup_id_list)]

    # Delete columns used for judgment
    drop_column = ['conclude_COMMON_NAME']
    df_dup = df_dup.drop(columns=drop_column)
    df_not_dup = df_not_dup.drop(columns=drop_column)

    return df_dup, df_not_dup


def filter_process(target_dir, save_dir):
    # Scan files and their data paths
    file_dict = utils.get_file_list(target_dir, {})

    # Filter target files for transfer
    target_file_list = ['enzrxns.dat.tsv',
                        'transporters.col.tsv']
    else_file_list = ['regulation.dat',
                      'regulons.dat', ]
    for file in file_dict:
        if file in target_file_list:
            new_end = ' bio' + utils.get_now_time() + '.tsv'
            new_file_name = file.replace('.tsv', new_end)

            data_path = file_dict.get(file)
            save_path = save_dir + new_file_name
            shutil.copy(data_path, save_path)
        if file in else_file_list:
            data_path = file_dict.get(file)
            save_path = save_dir + file
            shutil.copy(data_path, save_path)


def copy_protein_all(data_path, save_path):
    shutil.copy(data_path, save_path)


class ProteinNormalize:

    def __init__(self, ref_path, input_dir, output_dir):
        self.ref_path = ref_path
        self.input_dir = os.path.join(input_dir, 'Data')
        self.root_dir = output_dir

        if not input_dir.endswith('/'):
            self.input_dir = self.input_dir + '/'
        if not input_dir.endswith('/'):
            self.root_dir = self.root_dir + '/'

    def biocyc_protein_1st_clean(self, protein_common_3rd_save_dir, protein_result_dir):
        path_links_dat = protein_common_3rd_save_dir + 'protein-links.dat.tsv'
        path_dat = protein_common_3rd_save_dir + 'proteins.dat.tsv'

        df_links_dat = utils.load_df(path_links_dat)
        df_dat = utils.load_df(path_dat)

        # Merge protein-links.dat and proteins.dat
        df_merge = merge_process(df_links_dat, df_dat)

        # Integrate enzyme_ID and UNIQUE-ID as Protein_BioCyc_ID
        df_merge = start_combine_id(df_merge)

        save_dir = protein_result_dir + '/Biocyc_protein_1st_result/'
        utils.create_dir(save_dir)

        save_path = protein_result_dir + '/Biocyc_protein_1st_result/protein_all_bio.tsv'
        utils.save_df(save_path, df_merge)

    def biocyc_protein_2nd_clean(self, protein_result_dir):
        # First Protein processing result
        path_1st_protein_all = protein_result_dir + '/Biocyc_protein_1st_result/protein_all_bio.tsv'
        df = utils.load_df(path_1st_protein_all)

        # Merge synonymes and SYNONYMS
        df = start_combine_synonyms(df)

        # Second Protein result storage folder
        save_dir = protein_result_dir + '/Biocyc_protein_2nd_result/'
        utils.create_dir(save_dir)
        save_path = save_dir + '2nd_protein_all_bio.tsv'
        utils.save_df(save_path, df)

        # Extract data where COMMON-NAME (case-insensitive) same and ecocyc_id has value and same
        df_dup, df_not_dup = filer_dup_COMMON_NAME_ecocyc_id(df)

        if len(df_dup) == 0:
            with open(save_dir + 'No eligible duplicate data found..txt', 'w+') as f:
                pass
        else:
            path_save_dup = save_dir + '2nd_protein_repeat_bio.tsv'
            path_save_unique = save_dir + '2nd_protein_all_bio_unique.tsv'
            utils.save_df(path_save_dup, df_dup)
            utils.save_df(path_save_unique, df_not_dup)
    def biocyc_protein_3rd_clean(self, protein_result_dir, protein_common_3rd_save_dir):
        data_dir = protein_common_3rd_save_dir
        save_dir = protein_result_dir + 'Biocyc_protein_3rd_result/'
        utils.create_dir(save_dir)
        # Get part belonging to Protein table from generic processing
        filter_process(data_dir, save_dir)

        # Load previous Protein processing result for 4th processing
        # Protein second processing result file
        path_2nd_clean_result = protein_result_dir + 'Biocyc_protein_2nd_result/2nd_protein_all_bio.tsv'

        path_3rd_clean_result = save_dir + '4th_protein_all_bio.tsv'

        copy_protein_all(path_2nd_clean_result, path_3rd_clean_result)

    def run(self):

        # Load BioCyc newly added bacterial group data
        path_ref = self.ref_path
        df_ref = utils.load_df(path_ref)

        # Storage directory for processing results
        save_dir_all = self.root_dir  # Main directory

        save_dir = save_dir_all + 'Data/'  # Data directory
        utils.create_dir(save_dir)

        # This processing process Record file
        record_dir = f'{save_dir_all}Record/'  # Record directory
        utils.create_dir(record_dir)
        record_path = record_dir + f'biocyc_filter_record.json'
        if os.path.exists(record_path):
            with open(record_path, 'r') as f:
                record_dict = json.load(f)
        else:
            record_dict = {}
        # Record error logs
        error_log_path = record_dir + 'error_log.txt'

        # match_name used in ref to determine if corresponding BioCyc data exists, if yes this column will have corresponding name
        df_ref = df_ref[df_ref['download_linkrepresentative_genome'] != 'NA_NO']

        # Extract file_name
        def exact_gz_name(download_link_representative_genome):
            temp_list = download_link_representative_genome.split('/')
            file_name = temp_list[-1]
            return file_name

        df_ref['BioCyc_gz_name'] = df_ref['download_linkrepresentative_genome'].apply(
            lambda x: exact_gz_name(x)
        )

        # Generate dictionary for subsequent setting of processing result file names
        fields_to_use = ['lishan_species_tax_id', 'lishan_strains_tax_id']
        result_file_name_dict = df_ref.set_index('BioCyc_gz_name')[fields_to_use].T.to_dict()

        new_gz_file_list = df_ref['BioCyc_gz_name'].unique().tolist()

        # Error information record
        errors = []

        for file in new_gz_file_list:

            # Get ID for distinguishing different bacteria during file storage
            one_result_dict = result_file_name_dict.get(file)
            li_shan_species_id = one_result_dict.get('lishan_species_tax_id')
            li_shan_strains_id = one_result_dict.get('lishan_strains_tax_id')
            save_file_id = li_shan_species_id + '_' + li_shan_strains_id

            if save_file_id in record_dict:
                deal_status = record_dict.get(save_file_id)
                if deal_status == 'Success':
                    print(f'{save_file_id}, This one had {deal_status} before!')
                    continue

            # Record error information and error location
            d = {save_file_id: 'Success'}
            print(save_file_id, end=': ')
            print(file)

            # Step2_protein_deal
            try:

                save_id = save_file_id

                # Storage folder for this Protein processing results
                protein_result_dir = save_dir + save_id + '/Protein/'

                # Storage folder for gene-related files of generic processing 4th results
                protein_common_3rd_save_dir = self.input_dir + save_id + f'/Biocyc_3rd_result/Data/protein/'

                # 1. Merge protein-links.dat and proteins.dat; integrate enzyme_ID and UNIQUE-ID as Protein_BioCyc_ID
                print('Protein')
                print('###########')
                print('1. Merge protein-links.dat and proteins.dat; integrate enzyme_ID and UNIQUE-ID as Protein_BioCyc_ID')
                self.biocyc_protein_1st_clean(
                    protein_common_3rd_save_dir=protein_common_3rd_save_dir,
                    protein_result_dir=protein_result_dir
                )
                print('###########')
                print('2. Merge synonymes and SYNONYMS; extract data where COMMON-NAME (case-insensitive) same and ecocyc_id has value and same')
                self.biocyc_protein_2nd_clean(
                    protein_result_dir=protein_result_dir
                )
                print('###########')
                print('4. Filter files by specified file directory')
                self.biocyc_protein_3rd_clean(
                    protein_result_dir=protein_result_dir,
                    protein_common_3rd_save_dir=protein_common_3rd_save_dir
                )
                print('###########')
            except Exception as e:
                error_message = f"{str(save_file_id)}, Error occurred in Biocyc_protein_deal: {str(e)}"
                errors.append(error_message)
                d = {save_file_id: 'Fail'}

            # If error information exists, write to file
            if errors:
                with open(error_log_path, "a+") as f:
                    for error in errors:
                        f.write(error + "\n")
                print("Errors have been logged in error_log.txt")
            else:
                print("All functions executed successfully.")

            with open(record_path, 'w+') as f:
                record_dict.update(d)
                json.dump(record_dict, f, indent=4)


