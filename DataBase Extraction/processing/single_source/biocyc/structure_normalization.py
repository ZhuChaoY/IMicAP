import ast
import json
import os
import shutil
import chardet
import numpy as np
import pandas as pd
import tarfile
import utils.my_df_function as utils


# Detect file encoding
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']


def get_biocyc_columns():
    gene_links_dat = ['GENE-ID(EGby-nameCGSC-ID)',
                      'UniProt-ID',
                      'GENE-NAME']

    pathway_links_dat = ['Pathway biocyc ID',
                         'Pathway name',
                         'Synonyms',
                         'Synonyms',
                         'Synonyms',
                         'Synonyms',
                         'Synonyms',
                         'Synonyms',
                         'Synonyms',
                         'Synonyms',
                         'Synonyms']

    reaction_links_dat = ['biocyc ID',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)',
                          'EC Number (reaction nb)']

    protein_links_dat = ['enzyme_ID',
                         'ecocyc_id',
                         'uniprot_ID',
                         'enzyme name',
                         'synonymes']

    dict_columns = {'gene-links.dat.tsv': gene_links_dat,
                    'pathway-links.dat.tsv': pathway_links_dat,
                    'reaction-links.dat.tsv': reaction_links_dat,
                    'protein-links.dat.tsv': protein_links_dat}

    return dict_columns


def supply_columns(target_dir):
    dict_columns = get_biocyc_columns()
    # print(dict_columns)

    for dir_path, dir_names, filenames in os.walk(target_dir):
        for filename in filenames:
            save_dir = dir_path.replace('1st', '2nd')
            utils.create_dir(save_dir)

            file_path = dir_path + '/' + filename

            save_path = save_dir + '/' + filename
            # Supplement column names
            if filename in dict_columns:
                print(filename)
                # Load column names to add
                column_list = dict_columns.get(filename)
                str_column_list = '\t'.join(column_list)

                # Load encoding format
                encoding = 'utf-8'
                with open(file_path, 'r', encoding=encoding) as f:
                    data_content = f.read()

                # Supplement column names, resave data
                with open(save_path, 'w+', encoding=encoding) as f:
                    f.write(str_column_list)
                    f.write('\n')
                    f.write(data_content)
            elif filename == 'compounds.dat.tsv':
                # Load encoding format
                encoding = detect_encoding(file_path)
                df = utils.load_df(file_path, encode=encoding)
                rename_column = {"GIBBS-0": "Standard Gibbs Free Energy (ΔrG'°)"}
                df = df.rename(columns=rename_column)
                # print(df.columns.tolist())
                utils.save_df(save_path, df)
            else:
                shutil.copy(file_path, save_path)


# Scan target directory, which tsv files have DBLINKS field
def scan_dblink(target_dir):
    list_contains_dir = []

    for dir_path, dir_names, filenames in os.walk(target_dir):
        if 'else' in dir_path:
            continue

        for filename in filenames:
            # print(filename)
            if filename.endswith('.tsv'):
                data_path = dir_path + '/' + filename
                encode = detect_encoding(data_path)
                df = utils.load_df(data_path, encode)
                column_list = df.columns.tolist()
                if 'DBLINKS' in column_list:
                    print(filename)
                    list_contains_dir.append(filename)


# Sort dictionary by keys
def sort_dictionary(key_value):
    # sorted(key_value) returns re-sorted list
    # Dictionary sorted by keys
    new_key_value = {}
    for i in sorted(key_value):
        value = key_value[i]
        key = i

        if type(value) == list:
            value.sort()

        d = {key: value}
        new_key_value.update(d)

    return new_key_value


# Detailed character split processing process for DBLINKS
def str_dblink_deal_process(dblink, unique_id):
    new_dblink = dblink.replace('(', '')
    list_str_dblink = new_dblink.split(' ')

    # Keys needing attention
    attention_key = ['ECOCYC', 'METACYC', 'BIOCYC']

    # Build key key
    if len(list_str_dblink) >= 3:
        if list_str_dblink[2] == 'NIL':
            key_data = list_str_dblink[0]
        else:
            key_data = list_str_dblink[0] + ' ' + list_str_dblink[2]
    else:
        key_data = list_str_dblink[0]

    # Get value value
    value_data = list_str_dblink[1].replace('"', '')
    value_data = value_data.replace(')', '')

    # ': ' link key and value
    final_dblink_str = key_data + ': ' + value_data

    # Determine if key is key we need special attention
    if key_data in attention_key:
        if str(value_data) == str(unique_id):
            final_dblink_str = np.nan

    return final_dblink_str


# Detailed processing process
def dblinks_deal_process(dblink, unique_id):
    if pd.isnull(dblink):
        return dblink

    try:
        transform_dblink = ast.literal_eval(dblink)

        if type(transform_dblink) == list:
            list_dblink = transform_dblink

            final_list = []
            for dblink_item in list_dblink:
                dblink_item_str = str_dblink_deal_process(dblink_item, unique_id)

                if pd.isnull(dblink_item_str):
                    pass
                else:
                    final_list.append(dblink_item_str)

            final_dblink_str = utils.change_list_to_special_data(final_list)
            return final_dblink_str

        else:
            print('Other types of dblinks appear:')
            print(transform_dblink)
            print('#########')
            final_dblink_str = str_dblink_deal_process(dblink, unique_id)
    except:
        final_dblink_str = str_dblink_deal_process(dblink, unique_id)

    return final_dblink_str


# Rename dblink

# Start convert dblink
def start_dblinks_transforms(data_dir):
    target_file_dict = {
        'genes.dat.tsv': 'UNIQUE-ID',
        'compounds.dat.tsv': 'UNIQUE-ID',
        'proteins.dat.tsv': 'UNIQUE-ID'
    }

    # Scan for conditions meeting requirements in folder
    for dir_path, dir_list, filenames in os.walk(data_dir):

        save_dir = dir_path.replace('2nd', '3rd')
        utils.create_dir(save_dir)

        for filename in filenames:
            file_path = dir_path + '/' + filename
            save_path = save_dir + '/' + filename
            if filename in target_file_dict:
                print(f'---{filename}---')

                column_id = 'UNIQUE-ID'

                try:
                    df = utils.load_df(file_path)
                except:
                    encode = detect_encoding(file_path)
                    df = utils.load_df(file_path, encode=encode)
                    print(encode)
                column_list = df.columns.to_list()
                if 'DBLINKS' in column_list:
                    df['DBLINKS'] = df.apply(lambda x: dblinks_deal_process(x['DBLINKS'], column_id), axis=1)
                else:
                    df['DBLINKS'] = np.nan

                utils.save_df(save_path, df)
            else:
                shutil.copy(file_path, save_path)


def get_file_path_list(path, path_list):
    if os.path.isdir(path):
        file_list = os.listdir(path)
        for file in file_list:
            new_path = path + '/' + file
            path_list = get_file_path_list(new_path, path_list)
    else:
        path_list.append(path)

    return path_list


# Scan column name situation
def scan_tsv_columns(data_dir, save_dir):
    path_list = get_file_path_list(data_dir, [])
    tsv_columns_dict = {}  # Initialize column name summary result

    for path in path_list:

        if '/else/' in path:  # Exclude else files
            continue
        if not path.endswith('.tsv'):  # Exclude non-tsv files
            continue
        if '_3rd_' not in path:
            continue

        print(path)

        temp_list = path.split('/')
        file_name = temp_list[-1]

        try:
            df = utils.load_df(path)
        except:
            df = utils.load_df(path, encode='gbk')
        columns_list = df.columns.to_list()

        if file_name not in tsv_columns_dict:
            tsv_columns_dict[file_name] = columns_list
        else:
            old_columns_list = tsv_columns_dict.get(file_name)
            for col in columns_list:
                if col not in old_columns_list:
                    old_columns_list.append(col)
            tsv_columns_dict[file_name] = columns_list

    save_name = 'Tsv_columns_scan_summary.json'
    utils.create_dir(save_dir)
    save_path = save_dir + save_name
    with open(save_path, 'w+') as f:
        json.dump(tsv_columns_dict, f, indent=4)

    return tsv_columns_dict


# Unify column names of each tsv file
def unite_tsv_columns(data_dir, tsv_columns_dict):
    path_list = get_file_path_list(data_dir, [])

    for path in path_list:
        # Initialize storage path
        temp_list = path.split('/')
        file_name = temp_list[-1]  # File name

        temp_list.pop(-1)
        temp_dir = '/'.join(temp_list)

        if '3rd' not in path:  # Exclude data other than 3rd processing results
            continue
        save_dir = temp_dir.replace('_3rd_', '_4th_')
        print(save_dir)
        utils.create_dir(save_dir)
        save_path = save_dir + '/' + file_name

        if '/else/' in path:  # Exclude else files
            shutil.copy(path, save_path)
            continue
        if not path.endswith('.tsv'):  # Exclude non-tsv files
            shutil.copy(path, save_path)
            continue

        print(path)
        if file_name in tsv_columns_dict:
            unite_columns_list = tsv_columns_dict.get(file_name)  # Get unified column headers
            try:
                df = utils.load_df(path)
            except:
                df = utils.load_df(path, encode='gbk')
            # Add all missing columns at once
            missing_cols = set(unite_columns_list) - set(df.columns)
            if missing_cols:
                df = df.reindex(columns=df.columns.tolist() + list(missing_cols))

            utils.save_df(save_path, df)


class StructureNormalization:
    def __init__(self, ref_path, input_dir, output_dir):
        self.ref_path = ref_path
        self.input_dir = input_dir
        self.output_dir = output_dir

        if not input_dir.endswith('/'):
            self.input_dir = self.input_dir + '/'
        if not input_dir.endswith('/'):
            self.output_dir = self.output_dir + '/'

    @staticmethod
    # Type 1, remove # starting description lines, split each line data by '//'
    def change_table_type_1(file_path, save_dir, save_path):
        encoding = detect_encoding(file_path)

        with open(file_path, 'r', encoding=encoding) as f:
            input_text = f.read()

        # Step 1: Delete text starting with '#' at data beginning
        lines = input_text.split('\n')
        new_line = []
        for line in lines:
            if line.startswith('#'):
                continue
            else:
                new_line.append(line)

        # Step 2: Split each line data by '//'
        data_str = '\n'.join(new_line)
        records = data_str.split('\n//\n')

        # Step 3: Convert data to tsv file data
        list_dict = []
        for record in records:
            # Initialize dict to store each line data values
            data_line_dict = {}

            # Skip empty data
            record = record.strip()
            if not record:
                continue  # Skip empty records

            # Used to record number of times a key appears
            dict_key_appear = {}
            pre_key = None

            # Split Record into lines collection, then process into key-value pairs
            lines = record.split('\n')
            for line in lines:

                # Ignore empty data
                # line = line.strip()
                if len(line) == 0:
                    continue

                if line.startswith('/') or line.startswith('//'):
                    # print('Split problem appears')
                    if pre_key is None:
                        print('Split problem appears')
                        print(record)
                        print('######################')
                        continue

                    # if pre_key not in temp_column_list:
                    #     print('column', end=': ')
                    #     print(pre_key)
                    #     # print(record)
                    #     print('##########')
                    #     temp_column_list.append(pre_key)

                    data_index = dict_key_appear.get(pre_key) - 1
                    pre_data_list = data_line_dict.get(pre_key)

                    now_data = line.strip('/')

                    if type(pre_data_list) == list:
                        pre_data = pre_data_list[data_index]
                        new_data = pre_data + now_data
                        pre_data_list[data_index] = new_data

                        new_data_list = pre_data_list
                        d = {pre_key: new_data_list}
                        data_line_dict.update(d)
                    else:
                        # Determine if previous data empty
                        if pd.isnull(pre_data_list):
                            d = {pre_key: now_data}
                            data_line_dict.update(d)
                        else:
                            pre_data = pre_data_list
                            new_data = pre_data + ' ' + now_data
                            d = {pre_key: new_data}
                            data_line_dict.update(d)

                else:
                    # Handle key-value
                    pair = line.split(' - ')

                    # Only column name situation
                    if len(pair) == 1:
                        key_data = pair[0]
                        # print(record)
                        # print(line)
                        # print(pair)
                        # print(key_data)

                        value_data = np.nan
                        d = {key_data: value_data}
                        data_line_dict.update(d)
                        continue
                    # Situation with column name and value
                    else:
                        key_data = pair.pop(0)
                        value_data = ' - '.join(pair)

                    # Reset most recent key value
                    pre_key = key_data

                    # Determine if multi-value situation
                    if key_data in data_line_dict:
                        # Handle multi-value issue
                        old_value = data_line_dict.get(key_data)
                        if type(old_value) == list:
                            old_value.append(value_data)
                            new_value = old_value
                        else:
                            if pd.isnull(old_value):
                                new_value = value_data
                            else:
                                new_value = [old_value, value_data]
                        d = {key_data: new_value}
                        data_line_dict.update(d)

                        # Update key appearance count
                        appear_count = dict_key_appear.get(key_data)
                        new_count_dict = {key_data: appear_count + 1}
                        dict_key_appear.update(new_count_dict)
                    else:
                        d = {key_data: value_data}
                        data_line_dict.update(d)

                        # Update key appearance count
                        appear_count = 1
                        new_count_dict = {key_data: appear_count}
                        dict_key_appear.update(new_count_dict)

            # Save data result
            list_dict.append(data_line_dict)

        # step 4: Build df tsv file
        df = pd.DataFrame(list_dict)

        utils.create_dir(save_dir)
        utils.save_df(save_path, df, encoding=encoding)

    @staticmethod
    # Type 2, itself has column headers, only need delete '#' starting description information
    def change_table_type_2(file_path, save_dir, save_path):
        encoding = detect_encoding(file_path)
        # print(encoding)

        with open(file_path, 'r', encoding=encoding) as f:
            input_text = f.read()

        # Step 1: Delete text starting with '#' at data beginning
        lines = input_text.split('\n')
        new_line = []
        mark_not_special = True
        for line in lines:
            if mark_not_special is False:
                new_line.append(line)
            else:
                if line.startswith('#'):
                    continue
                else:
                    mark_not_special = False
                    new_line.append(line)

        str_record = '\n'.join(new_line)

        utils.create_dir(save_dir)
        with open(save_path, 'w+', encoding=encoding) as f:
            f.write(str_record)

    @staticmethod
    # Type 3, itself no column headers, need supplement column headers, simultaneously need delete '#' starting description information
    def change_table_type_3(file_path, save_dir, save_path):
        encoding = detect_encoding(file_path)
        # print(encoding)

        with open(file_path, 'r', encoding=encoding) as f:
            input_text = f.read()

        # Step 1: Delete text starting with '#' at data beginning
        lines = input_text.split('\n')
        new_line = []
        mark_not_special = True
        for line in lines:
            if mark_not_special is False:
                new_line.append(line)
            else:
                if line.startswith('#'):
                    continue
                else:
                    mark_not_special = False
                    new_line.append(line)

        str_record = '\n'.join(new_line)

        utils.create_dir(save_dir)
        with open(save_path, 'w+', encoding=encoding) as f:
            f.write(str_record)

    @staticmethod
    # Type 4, unknown processing method
    def change_table_type_4(file_path, save_dir, save_path):
        utils.create_dir(save_dir)
        shutil.copyfile(file_path, save_path)

    # All tables overall processing
    def change_all_tables(self, original_dir, save_dir):
        type_1_files = ['enzrxns.dat',
                        'rnas.dat',
                        'transunits.dat',
                        'classes.dat',
                        'pubs.dat',
                        'species.dat',
                        'pathways.dat',
                        'reactions.dat',
                        'protein-features.dat',
                        'proteins.dat',
                        'compounds.dat',
                        'genes.dat'
                        ]
        type_2_files = ['enzymes.col',
                        'genes.col',
                        'pathways.col',
                        'protcplxs.col',
                        'transporters.col',
                        ]
        type_3_files = ['gene-links.dat',
                        'pathway-links.dat',
                        'reaction-links.dat',
                        'protein-links.dat',
                        'compound-links.dat',
                        ]
        type_4_files = ['dnabindsites.dat',
                        'promoters.dat',
                        'terminators.dat',
                        'protligandcplxes.dat',
                        'protseq.fsa',
                        'protseq.pfam',
                        'regulation.dat',
                        'regulons.dat',
                        ]

        # original_dir is original main directory, then scan subdirectories
        sub_dir_list = os.listdir(original_dir)

        for sub_dir in sub_dir_list:
            # Build subdirectory for storage use
            save_sub_dir = save_dir + '/' + sub_dir + '/'

            # Build subdirectory path
            sub_dir = original_dir + '/' + sub_dir + '/'

            # Scan directory, get file collection
            file_list = os.listdir(sub_dir)

            # Process by type separately
            for file in file_list:
                if file in type_1_files:
                    save_path = save_sub_dir + file + '.tsv'
                    file_path = sub_dir + file
                    self.change_table_type_1(file_path, save_sub_dir, save_path)
                elif file in type_2_files:
                    save_path = save_sub_dir + file + '.tsv'
                    file_path = sub_dir + file
                    self.change_table_type_2(file_path, save_sub_dir, save_path)
                elif file in type_3_files:
                    save_path = save_sub_dir + file + '.tsv'
                    file_path = sub_dir + file
                    self.change_table_type_3(file_path, save_sub_dir, save_path)
                else:
                    save_path = save_sub_dir + file
                    file_path = sub_dir + file
                    self.change_table_type_4(file_path, save_sub_dir, save_path)

    @staticmethod
    def classify_files_process(root_dir, target_dir):
        # Define file classification rules
        classification = {
            "enzyme": ["enzrxns.dat",
                       "enzymes.col"],
            "gene": ['gene-links.dat',
                     'genes.col',
                     'genes.dat',
                     'rnas.dat'],
            "path react": ['pathway-links.dat',
                           'pathways.col',
                           'pathways.dat',
                           'reaction-links.dat',
                           'reactions.dat'],
            "protein": ['protein-links.dat',
                        'proteins.dat',
                        'regulation.dat',
                        'regulons.dat',
                        'transporters.col'],
            "SM": ["compounds.dat"],
            'else': []
        }

        # So file collection needing division
        not_else_files = [
            'enzrxns.dat',
            'enzymes.col',
            'gene-links.dat',
            'genes.col',
            'genes.dat',
            'rnas.dat',
            'classes.dat',
            'pubs.dat',
            'species.dat',
            'pathway-links.dat',
            'pathways.col',
            'pathways.dat',
            'reaction-links.dat',
            'reactions.dat',
            'protein-links.dat',
            'proteins.dat',
            'regulation.dat',
            'regulons.dat',
            'transporters.col',
            'compounds.dat']

        # Create target folder and its subfolders
        for folder in classification.keys():
            os.makedirs(os.path.join(target_dir, folder), exist_ok=True)

        # Traverse root folder and its subfolders
        for subdir, _, files in os.walk(root_dir):
            for file in files:

                # Find file complete path
                file_path = os.path.join(subdir, file)
                # print(file_path)

                # Store files not belonging to any category into else folder
                if file not in not_else_files:
                    save_dir = target_dir + 'else/'
                    utils.create_dir(save_dir)
                    target_path = save_dir + file

                    # Move file
                    shutil.move(file_path, target_path)
                    # print(f"Moved {file_path} to {target_path}")
                    continue

                # Classify file to corresponding folder
                for folder, file_list in classification.items():
                    if file in file_list:
                        # Define target path
                        target_path = os.path.join(target_dir, folder, file)

                        # Move file
                        shutil.move(file_path, target_path)
                        # print(f"Moved {file_path} to {target_path}")

        print("Files have been successfully classified and moved.")

    @staticmethod
    def get_newest(dir_path):
        version_list = os.listdir(dir_path)

        first_conclude = True
        newest_version = 0
        for version in version_list:

            version = float(version)
            if first_conclude:
                newest_version = version
                first_conclude = False
            else:
                if version > newest_version:
                    newest_version = version
        return newest_version

    @staticmethod
    def extract_tar_gz(tar_gz_path, target_dir, file_name):
        target_dir = target_dir + file_name + '/'
        # Ensure target directory exists
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # Open tar.gz file
        with tarfile.open(tar_gz_path, "r:gz") as tar:
            print(target_dir)
            # # Extract all files to target directory
            # tar.extractall(path=target_dir)
            # print(f"Files extracted to: {target_dir}")
            for member in tar.getmembers():
                # Filter illegal characters
                valid_name = ''.join(c for c in member.name if c not in r':*?"<>|')
                member.name = valid_name
                tar.extract(member, path=target_dir)

    def extract_and_classify_biocyc(self, file_name, tar_gz_dir, first_result_dir):
        # 1. First extract files
        print('1. Start extraction')
        tar_gz_path = tar_gz_dir + file_name  # tar.gz file path
        tar_result_dir = first_result_dir + 'Prepare_Deal/Biocyc_tar_result/'
        self.extract_tar_gz(tar_gz_path, tar_result_dir, file_name)
        print('1. Extraction end')

        # 2. Classify and store extraction results
        print('2. Start classification storage')
        # Define root folder and target folder
        root_dir = tar_result_dir + file_name
        version = self.get_newest(root_dir)
        root_dir = root_dir + '/' + str(version) + '/'  # File path needing classification storage
        classify_result_dir = first_result_dir + "Prepare_Deal/Biocyc_classify_result/"  # Storage path for classification results
        self.classify_files_process(root_dir=root_dir, target_dir=classify_result_dir)
        print('2. Classification storage end')

        # 3. Table corresponding files
        print('3. Start tablize')
        original_dir = classify_result_dir  # Replace with actual target folder path
        table_result_dir = first_result_dir + 'Data/'
        self.change_all_tables(original_dir, table_result_dir)
        print('3. Tablize processing end')

    def add_missing_columns_biocyc(self, first_result_dir):
        # Data storage path for Biocyc first processing results
        biocyc_1st_result_data_dir = first_result_dir + 'Data/'

        # Supplement column names
        print('4. Start supplement column names')
        supply_columns(biocyc_1st_result_data_dir)
        print('4. Supplement column names end')

    def transform_dblinks_biocyc(self, second_deal_result_dir):
        print('5. Start process DBLINKS')
        target_dir = second_deal_result_dir
        start_dblinks_transforms(target_dir)
        print('5. DBLINKS processing end')

    def scan_and_unify_biocyc_columns(self, pre_res_dir, summary_dir):
        # Scan column name situation
        tsv_columns_dict = scan_tsv_columns(data_dir=pre_res_dir, save_dir=summary_dir)

        # Unify column names of each tsv file
        unite_tsv_columns(data_dir=pre_res_dir, tsv_columns_dict=tsv_columns_dict)

    def run(self):
        # Storage path for unextracted gz
        original_gz_dir = self.input_dir

        # Load BioCyc newly added bacterial group data
        path_ref = self.ref_path
        df_ref = utils.load_df(path_ref)

        # Storage directory for processing results
        save_dir_all = self.output_dir  # Main directory

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

        # Generic processing
        for file in new_gz_file_list:

            # Get ID for distinguishing different bacteria during file storage
            one_result_dict = result_file_name_dict.get(file)
            li_shan_species_id = one_result_dict.get('lishan_species_tax_id')
            li_shan_strains_id = one_result_dict.get('lishan_strains_tax_id')
            save_file_id = li_shan_species_id + '_' + li_shan_strains_id

            # if save_file_id in record_dict:
            #     deal_status = record_dict.get(save_file_id)
            #
            #     if deal_status == 'Success':
            #         print(f'{save_file_id}, This one had {deal_status} before!')
            #         continue

            # Step1_common_deal
            try:
                # Skip bacteria where corresponding file does not exist
                path_gz_file = original_gz_dir + file
                if not os.path.exists(path_gz_file):
                    return

                # Storage folder for preprocessing results
                first_result_dir = f"{save_dir}{save_file_id}/Biocyc_1st_result/"
                second_result_dir = f"{save_dir}{save_file_id}/Biocyc_2nd_result/Data/"

                # 1. File extraction, classification storage, tablize
                print('Common')
                print('##########')
                self.extract_and_classify_biocyc(
                    file_name=file,
                    tar_gz_dir=original_gz_dir,
                    first_result_dir=first_result_dir
                )
                print('##########')

                # 2. Add column names to tablized tables
                self.add_missing_columns_biocyc(first_result_dir)
                print('##########')

                # 3. Process DBLINKS field in data
                self.transform_dblinks_biocyc(
                    second_deal_result_dir=second_result_dir
                )
                print('##########')

            except Exception as e:
                error_message = f"{str(save_file_id)}, Error occurred in Biocyc_common_deal: {str(e)}"
                errors.append(error_message)

        # Generic processing - column header unification
        self.scan_and_unify_biocyc_columns(
            pre_res_dir=save_dir_all,
            summary_dir=save_dir_all + 'Summary/'
        )


