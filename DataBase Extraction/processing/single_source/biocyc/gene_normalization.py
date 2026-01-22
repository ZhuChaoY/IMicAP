import json
import os
import numpy as np
import pandas as pd
import ast
import utils.my_df_function as utils


def merge_table(target_dir, save_dir):
    utils.create_dir(save_dir)
    save_path = save_dir + '/gene_all_bio.tsv'

    path_gene_links_dat = target_dir + '/' + 'gene-links.dat.tsv'
    df_gene_links_dat = utils.load_df(path_gene_links_dat)
    rename_dict = {'GENE-ID(EGby-nameCGSC-ID)': 'GENE_UNIQUE_ID'}
    df_gene_links_dat = df_gene_links_dat.rename(columns=rename_dict)

    path_gene_dat = target_dir + '/' + 'genes.dat.tsv'
    df_gene_dat = utils.load_df(path_gene_dat)
    rename_dict = {'UNIQUE-ID': 'GENE_UNIQUE_ID',
                   'SYNONYMS': 'SYNONYMSgenesdat'}
    if 'SYNONYMS' not in df_gene_dat.columns:
        df_gene_dat['SYNONYMS'] = np.nan
    df_gene_dat = df_gene_dat.rename(columns=rename_dict)

    path_gene_col = target_dir + '/' + 'genes.col.tsv'
    df_gene_col = utils.load_df(path_gene_col)
    rename_dict = {'UNIQUE-ID': 'GENE_UNIQUE_ID'}
    df_gene_col = df_gene_col.rename(columns=rename_dict)

    df_final = pd.merge(left=df_gene_links_dat, right=df_gene_dat,
                        on='GENE_UNIQUE_ID', how='outer')
    df_final = pd.merge(left=df_final, right=df_gene_col,
                        on='GENE_UNIQUE_ID', how='outer')

    utils.save_df(save_path, df_final)


# Get potential list data
def get_potential_list(data):
    if pd.isnull(data):
        return data

    try:
        new_data = ast.literal_eval(data)
        if type(new_data) == list:
            return new_data
        else:
            return data
    except:
        return data


# If both columns exist, specific merge process
def combine_process(df):
    # Use ast.literal_eval to convert string to list
    df['ACCESSION-1'] = df['ACCESSION-1'].apply(lambda x: get_potential_list(x))
    df['ACCESSION-2'] = df['ACCESSION-2'].apply(lambda x: get_potential_list(x))

    # Merge ACCESSION-1 and ACCESSION-2 lists
    def combine_process(access_1, access_2):
        if pd.isnull(access_1):
            final_data = access_2
            if type(access_2) == list:
                return utils.change_list_to_special_data(final_data)
            else:
                return final_data

        if pd.isnull(access_2):
            final_data = access_1
            if type(access_1) == list:
                return utils.change_list_to_special_data(final_data)
            else:
                return final_data

        if type(access_1) != list:
            access_1 = [access_1]
        if type(access_2) != list:
            access_2 = [access_2]

        final_list = list(set(access_1 + access_2))
        final_str = utils.change_list_to_special_data(final_list)
        return final_str

    # List deduplication
    df['ACCESSION_ID'] = df.apply(lambda x: combine_process(x['ACCESSION-1'], x['ACCESSION-2']), axis=1)

    drop_column = ['ACCESSION-2', 'ACCESSION-1']
    df = df.drop(columns=drop_column)

    return df


# If only one column, only rename; if no column, add an empty column
def rename_process(df, contains_column):
    rename_dict = {contains_column: 'ACCESSION_ID'}
    df = df.rename(columns=rename_dict)
    return df


def ACCESSION_combine_process(df):
    columns_list = df.columns.tolist()

    if 'ACCESSION-1' in columns_list and 'ACCESSION-2' in columns_list:
        df = combine_process(df)
    elif 'ACCESSION-1' in columns_list and 'ACCESSION-2' not in columns_list:
        df = rename_process(df, 'ACCESSION-1')
    elif 'ACCESSION-2' in columns_list and 'ACCESSION-1' not in columns_list:
        df = rename_process(df, 'ACCESSION-2')
    else:
        df['ACCESSION_ID'] = np.nan

    return df


# Scan df to be merged, which column names start with SYNONYMS
def scan_synonyms(df):
    synonyms_columns = []

    column_list = df.columns.to_list()

    for column in column_list:

        if column.startswith('SYNONYMS'):
            synonyms_columns.append(column)
            print(synonyms_columns)
    if 'SYNONYMSgenesdat' in synonyms_columns:
        synonyms_columns.remove('SYNONYMSgenesdat')

    return synonyms_columns


# Start synonyms merge
def start_synonyms_combine(df):
    # Get SYNONYMS.x column names needing merge
    synonyms_columns = scan_synonyms(df)
    print(synonyms_columns)

    # Handle potential list type data in SYNONYMS.x
    for synonyms_column in synonyms_columns:
        df[synonyms_column] = df[synonyms_column].apply(lambda x: get_potential_list(x))
    df['SYNONYMSgenesdat'] = df['SYNONYMSgenesdat'].apply(lambda x: get_potential_list(x))

    def synonyms_combine_process(row):
        synonyms_gene_dat = row['SYNONYMSgenesdat']
        if type(synonyms_gene_dat) == list:
            pass
        elif pd.isnull(synonyms_gene_dat):
            synonyms_gene_dat = []
        elif type(synonyms_gene_dat) != list:
            synonyms_gene_dat = [synonyms_gene_dat]

        # Perform merge processing sequentially
        for synonyms_column in synonyms_columns:
            synonyms_data = row[synonyms_column]

            # Skip empty
            if pd.isnull(synonyms_data):
                continue

            # Merge two lists and deduplicate
            elif type(synonyms_data) == list:
                synonyms_gene_dat = list(set(synonyms_gene_dat + synonyms_data))

            # Merge list and str
            elif type(synonyms_data) == str:
                synonyms_data_list = [synonyms_data]
                synonyms_gene_dat = list(set(synonyms_gene_dat + synonyms_data_list))

            # Other potentially problematic situations
            else:
                print('Special type')
                print(synonyms_data)
                print('#########')
                synonyms_data_list = [synonyms_data]
                synonyms_gene_dat = list(set(synonyms_gene_dat + synonyms_data_list))

        if len(synonyms_gene_dat) == 0:
            return row
        elif len(synonyms_gene_dat) == 1:
            row['SYNONYMSgenesdat'] = synonyms_gene_dat[0]
            return row
        else:
            row['SYNONYMSgenesdat'] = utils.change_list_to_special_data(synonyms_gene_dat)
            return row

    df = df.apply(synonyms_combine_process, axis=1)

    df = df.drop(columns=synonyms_columns)

    # Rename Synonyms
    dict_rename = {'SYNONYMSgenesdat': 'Synonyms'}
    df = df.rename(columns=dict_rename)

    return df


def columns_deal_process(df):
    final_column_list = ['Gene_biocyc_ID',
                         'UniProt-ID',
                         'gene_name',
                         'TYPES',
                         'ACCESSION_ID',
                         'CENTISOME-POSITION',
                         'COMPONENT-OF',
                         'INSTANCE-NAME-TEMPLATE',
                         'LEFT-END-POSITION',
                         'PRODUCT',
                         'RIGHT-END-POSITION',
                         'SOURCE-ORTHOLOG',
                         'Synonyms',
                         'TRANSCRIPTION-DIRECTION',
                         'DBLINKS',
                         'ABBREV-NAME',
                         'PRODUCT-NAME',
                         'REPLICON',
                         'START-BASE',
                         'END-BASE']

    dict_change_column = {'GENE_UNIQUE_ID': 'Gene_biocyc_ID',
                          'GENE-NAME': 'gene_name'}
    df = df.rename(columns=dict_change_column)

    original_column_list = df.columns.tolist()

    print('Fields not in gene_all_bio.tsv:')
    for column in final_column_list:
        if column not in original_column_list:
            print('\t', end='')
            print(column)
            df[column] = np.nan

    print('Fields only gene_all_bio.tsv has')
    for column in original_column_list:
        if column not in final_column_list:
            print('\t', end='')
            print(column)

    df = df[final_column_list]

    return df


def start_take_apart(df_gene, df_rna):
    list_rna_GENE = df_rna['GENE'].unique().tolist()

    # Filter out gene information related to RNA in gene table
    df_gene_of_rna = df_gene[df_gene['GENE_UNIQUE_ID'].isin(list_rna_GENE)]

    # Filter out gene information unrelated to RNA in gene table
    df_gene_none_about_rna = df_gene[-df_gene['GENE_UNIQUE_ID'].isin(list_rna_GENE)]

    return df_gene_of_rna, df_gene_none_about_rna


# Determine if CGSC-ID and DBLINKS are completely identical
def compare_CGSC_DBLINKS(df, path_check_res, path_different_save):
    # Determine if CGSC-ID column exists
    columns_list = df.columns.to_list()
    if 'CGSC-ID' not in columns_list:
        with open(path_check_res, 'w+') as f:
            print('CGSC-ID column does not exist')
            f.write('CGSC-ID column does not exist')
            return df

    # Below are situations where this bacteria's gene_all file has CGSC-ID column
    # For storing occurring situations
    conclude_result = []

    # Determine if two columns completely identical
    def conclude_process(cgsc_id, dblinks):

        # First judge if have, situations where only one side has value
        if pd.isnull(cgsc_id):
            if pd.notnull(dblinks):
                if 'CGSC' not in dblinks:
                    condition = 'Exist CGSC-ID empty; DBLINKS has "CGSC" situation'
                    if condition not in conclude_result:
                        conclude_result.append(condition)
                        return False
        if pd.isnull(dblinks) or 'CGSC' not in dblinks:
            if pd.notnull(cgsc_id):
                condition = 'Exist DBLINKS empty, or does not have "CGSC" information, but CGSC-ID has value situation'
                if condition not in conclude_result:
                    conclude_result.append(condition)
                    return False

        # Both sides have values judge if identical
        dblinks = utils.transform_back_to_list(dblinks)
        dblinks_cgsc = ''
        if type(dblinks) == str:
            list_item = dblinks.split(': ')
            dblinks_cgsc = list_item[1]

        elif type(dblinks) == list:
            for item in dblinks:
                if 'CGSC' in item:
                    list_item = dblinks.split(': ')
                    dblinks_cgsc = list_item[1]

        if dblinks_cgsc == cgsc_id:
            return True
        else:
            condition = 'Exist CGSC-ID not empty; DBLINKS has "CGSC"; but two not identical situation'
            if condition not in conclude_result:
                conclude_result.append(condition)
            return False

    df['is_consistent'] = df.apply(
        lambda x: conclude_process(x['CGSC-ID'], x['DBLINKS']), axis=1
    )

    if len(conclude_result) == 0:
        # Completely consistent situation
        drop_column = ['CGSC-ID', 'is_consistent']
        df = df.drop(columns=drop_column)
        conclude_result = ['CGSC-ID and DBLINKS completely consistent']
    else:
        # Difference situation
        df_different = df[df['is_consistent'] == False]
        utils.save_df(path_different_save, df_different)

        # Delete discriminant columns
        drop_column = ['is_consistent']
        df = df.drop(columns=drop_column)

    # Save judgment result
    with open(path_check_res, 'w+') as f:
        for item in conclude_result:
            f.write(item)
            f.write('\n')

    print(conclude_result)
    return df


class GeneNormalize:

    def __init__(self, ref_path, input_dir, out_dir):
        self.ref_path = ref_path
        self.input_dir = os.path.join(input_dir, 'Data')
        self.root_dir = out_dir

        if not input_dir.endswith('/'):
            self.input_dir = self.input_dir + '/'
        if not input_dir.endswith('/'):
            self.root_dir = self.root_dir + '/'

    def biocyc_gene_1st_clean(self, target_gene_dir, gene_result_dir):
        gene_1st_result_dir = gene_result_dir + 'Biocyc_gene_1st_result/'
        merge_table(target_gene_dir, gene_1st_result_dir)

    def biocyc_gene_2nd_clean(self, gene_result_dir):
        gene_1st_result = gene_result_dir + 'Biocyc_gene_1st_result/gene_all_bio.tsv'
        print(gene_1st_result)

        gene_2nd_save_dir = gene_result_dir + '/Biocyc_gene_2nd_result/'
        gene_2nd_save_path = gene_2nd_save_dir + '/gene_all_bio.tsv'
        df = utils.load_df(gene_1st_result)

        df = ACCESSION_combine_process(df)
        df = start_synonyms_combine(df)

        utils.create_dir(gene_2nd_save_dir)
        utils.save_df(gene_2nd_save_path, df)

    def biocyc_gene_3rd_clean(self, li_shan_id, gene_result_dir, common_3rd_gene_save_dir):
        # Sample parameters
        lishan_strain_tax_id = li_shan_id
        path_gene_2nd_result = gene_result_dir + '/Biocyc_gene_2nd_result/gene_all_bio.tsv'
        df_gene = utils.load_df(path_gene_2nd_result)
        path_common_3rd_rna = common_3rd_gene_save_dir + '/rnas.dat.tsv'
        df_rna = utils.load_df(path_common_3rd_rna)

        save_dir = gene_result_dir + '/Biocyc_gene_3rd_result/'
        utils.create_dir(save_dir)

        # Compare if CGSC-ID and DBLINKS completely identical
        path_check = save_dir + 'Compare of CGSC-ID and DBLINKS.txt'
        path_different_save = save_dir + lishan_strain_tax_id + '_gene_all_check_CGCS.tsv'
        df_gene = compare_CGSC_DBLINKS(df_gene, path_check_res=path_check, path_different_save=path_different_save)

        # Split gene_all table
        save_path_gene_of_rna = save_dir + lishan_strain_tax_id + '_RNA_gene_bio.tsv'
        save_path_none_about_rna = save_dir + lishan_strain_tax_id + '_gene_bio.tsv'
        df_gene_of_rna, df_gene_none_about_rna = start_take_apart(df_gene, df_rna)

        # Coordinate column names
        df_gene_none_about_rna = columns_deal_process(df_gene_none_about_rna)
        df_gene_of_rna = columns_deal_process(df_gene_of_rna)

        utils.save_df(save_path_gene_of_rna, df_gene_of_rna)
        utils.save_df(save_path_none_about_rna, df_gene_none_about_rna)

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

            # Step2_gene_deal
            try:
                # Storage folder for this stage gene processing results
                gene_result_dir = save_dir + save_file_id + '/Gene/'

                # Storage folder for gene-related files of generic processing 4th results
                common_4th_gene_save_dir = self.input_dir + save_file_id + f'/Biocyc_4th_result/Data/gene/'

                # 1. Merge multiple gene tables to form gene_all table
                print('Gene')
                print('##########')
                print('1. Merge gene tables')
                self.biocyc_gene_1st_clean(
                    target_gene_dir=common_4th_gene_save_dir,
                    gene_result_dir=gene_result_dir,
                )
                print('##########')
                print('2. Process ACCESSION, SYNONYMS')
                self.biocyc_gene_2nd_clean(gene_result_dir)
                print('##########')
                print('3. Process gene column names and fields')
                self.biocyc_gene_3rd_clean(
                    save_file_id,
                    gene_result_dir=gene_result_dir,
                    common_3rd_gene_save_dir=common_4th_gene_save_dir
                )
                print('###########')
            except Exception as e:
                error_message = f"{str(save_file_id)}, Error occurred in Biocyc_gene_deal: {str(e)}"
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
