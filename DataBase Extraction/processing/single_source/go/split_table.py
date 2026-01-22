import os.path
import re
import numpy as np
import pandas as pd
import utils.my_df_function as utils


#   Merge values of multiple columns data
def combine_target_columns_by_list(row, target_columns, original_col):
    new_data_list = []
    # Convert non-list data all to list, convenient for subsequent merge
    for col in target_columns:
        data = row[col]
        if pd.isnull(data):
            data = []
        elif not isinstance(data, list):
            data = [data]
        new_data_list.append(data)

    # Merge all data
    combine_data = []
    for data in new_data_list:
        combine_data = combine_data + data
        combine_data = list(set(combine_data))

    # Remove extra 'NA_NO'
    while 'NA_NO' in combine_data:
        combine_data.remove('NA_NO')

    # Convert data to specified multi-value string form
    if len(combine_data) == 0:
        combine_data = 'NA_NO'
    else:
        combine_data = utils.change_list_to_special_data(combine_data)
    row[original_col] = combine_data
    return row


def drop_wasted_cols(df):
    df = df[df['is_obsolete'] != 'true']
    drop_col = [
        'is_obsolete',
        'replaced_by',
        'consider',
        'consider_1',
        'consider_2',
        'consider_3',
        'consider_4',
        'consider_5',
        'consider_6',
        'consider_7',
    ]
    df = df.drop(columns=drop_col)
    return df


def combine_columns(df):
    type_of_combine_column = [
        'synonym',
        'alt_id',
        'xref',
        'subset'
    ]
    column_list = df.columns.to_list()
    for col in type_of_combine_column:
        original_col = col
        i_count = 1
        combine_columns = []
        print(df)
        while col in column_list:
            combine_columns.append(col)
            df[col] = df[col].apply(lambda x: utils.transform_back_to_list(x))
            col = f"{original_col}_{i_count}"
            i_count += 1

        print(combine_columns)
        # print(df)
        df = df.apply(
            lambda row: combine_target_columns_by_list(row, target_columns=combine_columns, original_col=original_col),
            axis=1)
    return df


def build_go_info(df):
    target_information_cols_part1 = [
        'id',
        'name',
        'namespace',
        'def',
        'synonym',
        'alt_id',
        'xref',
        'subset',
        'comment'
    ]
    df_info = df[target_information_cols_part1]
    return df_info


def unify_column(df_info):
    rename_dict = {
        'id': 'term_id',
        'name': 'term_name',
        'def': 'definition',
        'synonym': 'synonyms',
        'alt_id': 'alternate_id',
        'xref': 'dbxref',
    }
    df_info = df_info.rename(columns=rename_dict)
    df_info = df_info.fillna('NA_NO')
    return df_info


def build_go_relation(df):
    target_col = [
        'id',
        'name'
    ]
    column_type_list = [
        'is_a',
        'relationship'
    ]
    for col in column_type_list:
        i_count = 1
        original_col = col
        while col in df.columns:
            target_col.append(col)
            col = original_col + f'_{i_count}'
            i_count += 1
    df_relationship = df[target_col]
    return df_relationship


def split_isa(df):
    def spilt_process(row, target_col):
        row['relationship_type'] = np.nan
        row['term_id_2'] = np.nan
        row['term_name_2'] = np.nan

        data = row[target_col]
        if pd.isnull(data):
            return row

        temp_list = data.split(' ! ')
        row['relationship_type'] = 'is_a'
        row['term_id_2'] = temp_list[0]
        row['term_name_2'] = temp_list[1]

        return row

    df_final = pd.DataFrame()
    original_target_col = 'is_a'
    target_col = original_target_col
    i_count = 1
    while target_col in df.columns:

        columns_list = ['term_id_1', 'term_name_1', target_col]
        df_for_split = df[columns_list].copy()
        df_for_split = df_for_split[df_for_split[target_col].notna()].copy()

        if len(df_for_split) > 0:
            df_split_res = df_for_split.apply(lambda row: spilt_process(row, target_col=target_col), axis=1)
            final_columns_list = ['term_id_1', 'term_name_1', 'relationship_type', 'term_id_2', 'term_name_2']
            df_split_res = df_split_res[final_columns_list]
            df_final = pd.concat([df_final, df_split_res])

        target_col = f'{target_col}_{i_count}'
        i_count += 1
    return df_final


def split_relationship(df):
    def spilt_process(row, target_col):
        row['relationship_type'] = np.nan
        row['term_id_2'] = np.nan
        row['term_name_2'] = np.nan

        data = row[target_col]
        if pd.isnull(data):
            return row

        partter_terms_type = r'(.*) GO:'
        match = re.search(partter_terms_type, data)
        if match:
            terms_type = match.group(1).strip()
            row['relationship_type'] = terms_type

        partter_terms_id = r'GO:(.*)!'
        match = re.search(partter_terms_id, data)
        if match:
            terms_id = match.group(1).strip()
            row['term_id_2'] = f"GO:{terms_id}"

        temp_list = data.split(' ! ')
        row['term_name_2'] = temp_list[1]

        return row

    df_final = pd.DataFrame()
    original_target_col = 'relationship'
    target_col = original_target_col
    i_count = 1
    while target_col in df.columns:

        columns_list = ['term_id_1', 'term_name_1', target_col]
        df_for_split = df[columns_list]
        df_for_split = df_for_split[df_for_split[target_col].notna()]

        if len(df_for_split) > 0:
            df_split_res = df_for_split.apply(lambda row: spilt_process(row, target_col=target_col), axis=1)
            final_columns_list = ['term_id_1', 'term_name_1', 'relationship_type', 'term_id_2', 'term_name_2']
            df_split_res = df_split_res[final_columns_list]
            df_final = pd.concat([df_final, df_split_res])

        target_col = f'{target_col}_{i_count}'
        i_count += 1
    return df_final


def run(df, output_dir):
    utils.create_dir(output_dir)

    df = drop_wasted_cols(df)
    df = combine_columns(df)

    df_info = build_go_info(df)
    df_info = unify_column(df_info)
    save_name = f'GO_terms_information.tsv'
    save_path = os.path.join(output_dir, save_name)
    utils.save_df(save_path, df_info)

    df_relationship = build_go_relation(df)
    rename_dict = {
        'id': 'term_id_1',
        'name': 'term_name_1'
    }
    df_relationship = df_relationship.rename(columns=rename_dict)

    df_split_isa = split_isa(df_relationship)
    df_split_relationship = split_relationship(df_relationship)

    df_final = pd.concat([df_split_isa, df_split_relationship])
    df_final = df_final.fillna('NA_NO')

    save_name = f'GO_terms_relationship.tsv'
    save_path = os.path.join(output_dir, save_name)
    utils.save_df(save_path, df_final)

    return df_info, df_relationship
