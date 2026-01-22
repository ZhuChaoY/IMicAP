import ast
import os.path
import numpy as np
import pandas as pd
import utils.my_df_function as utils


# First match, via GENE
def match_by_GENE(rna_dat_df, gene_rna_df):
    gene_rna_df = gene_rna_df[-gene_rna_df['Gene_biocyc_ID'].isnull()]

    # Handle identical column names
    rna_column_list = rna_dat_df.columns.to_list()
    gene_column_list = gene_rna_df.columns.tolist()
    for column in rna_column_list:
        if column in gene_column_list:
            new_column = column + '_RNA'
            new_dict = {column: new_column}
            rna_dat_df = rna_dat_df.rename(columns=new_dict)
    for column in gene_column_list:
        if column in rna_column_list:
            new_column = column + '_gene'
            new_dict = {column: new_column}
            gene_rna_df = gene_rna_df.rename(columns=new_dict)

    # Use merge function to merge two DataFrames by "GENE" column and "Gene_biocyc_ID" column
    merged_data = pd.merge(rna_dat_df, gene_rna_df, how="left", left_on="GENE", right_on="Gene_biocyc_ID")

    return merged_data, rna_dat_df, gene_rna_df


# Second match, via UNMODIFIED_FORM
def match_by_UNMODIFIED_FORM(merge_df, rna_dat_df, gene_rna_df):
    gene_rna_df = gene_rna_df[gene_rna_df['Gene_biocyc_ID'].notnull()].copy()

    # Process UNMODIFIED_FORM column, remove last '-' and content after it before bij
    def deal_UNMODIFIED_FORM(unmodified_form):
        if pd.isnull(unmodified_form):
            return ""
        else:
            list_unmodified_form = unmodified_form.split('-')
            list_unmodified_form.pop(-1)
            new_unmodified_form = '-'.join(list_unmodified_form)
            return new_unmodified_form

    # Ensure column is string type
    rna_dat_df['UNMODIFIED-FORM_conclude'] = rna_dat_df.apply(
        lambda x: deal_UNMODIFIED_FORM(x['UNMODIFIED-FORM']), axis=1
    )

    # Ensure Gene_biocyc_ID is string type
    gene_rna_df['Gene_biocyc_ID'] = gene_rna_df['Gene_biocyc_ID'].astype(str)

    # Filter out data where previous GENE match failed
    df_fail_by_GENE = merge_df[merge_df['Gene_biocyc_ID'].isnull()]
    list_fail_UNIQUE_ID = df_fail_by_GENE['UNIQUE-ID'].tolist()

    # rna_dat_df previously failed part
    rna_dat_df_fail_before = rna_dat_df[rna_dat_df['UNIQUE-ID'].isin(list_fail_UNIQUE_ID)]

    # Perform second match on first failed part
    second_merge_df = pd.merge(
        rna_dat_df_fail_before,
        gene_rna_df,
        how="left",
        left_on="UNMODIFIED-FORM_conclude",
        right_on="Gene_biocyc_ID"
    )

    drop_column = ['UNMODIFIED-FORM_conclude']
    second_merge_df = second_merge_df.drop(columns=drop_column)

    # Part that still failed in second match
    fail_second_time_df = second_merge_df[second_merge_df['Gene_biocyc_ID'].isnull()]

    # Part successful in first merge result
    first_merge_success_df = merge_df[merge_df['Gene_biocyc_ID'].notnull()]

    # Merge second match result of first merge failure part with first match successful part
    final_df = pd.concat([first_merge_success_df, second_merge_df])

    return final_df, fail_second_time_df


# Check if a column contains list type data
def is_list_in_column(df, column_name):
    """
    Check if specified column contains list type data
    """
    if column_name not in df.columns:
        return False

    for value in df[column_name]:
        try:
            new_value = ast.literal_eval(value)
            if type(new_value) == list:
                return True
        except:
            pass

    return False


# Handle multi-value situation
def deal_multiply_data(save_path):
    # Load data
    df = pd.read_csv(save_path, sep='\t', index_col=False, encoding='utf-8')

    # Check each column sequentially for containing lists
    column_contains_list = []
    column_list = df.columns.to_list()
    for column in column_list:
        is_contains_list = is_list_in_column(df, column)
        if column == 'COMPONENT-OF':
            print(f"is_contains_list:{is_contains_list}")
        if is_contains_list:
            column_contains_list.append(column)

    print(f"column_contains_list: {str(column_contains_list)}")

    # Reload data
    df = utils.load_df(save_path)

    # Convert columns with list type data
    def change_to_special_data(data):
        if pd.isnull(data):
            return data
        try:
            list_data = ast.literal_eval(data)
            if type(list_data) == list:
                final_data = utils.change_list_to_special_data(list_data)
                return final_data
            else:
                return data
        except:
            return data

    for column in column_contains_list:
        df[column] = df[column].apply(lambda x: change_to_special_data(x))

    utils.save_df(save_path, df)
    return df


# Delete specified columns, generate new file
def drop_rna_dat_columns(df_rna_dat, save_path):
    drop_columns = ['SPECIES', 'Species LPSN']

    column_list = df_rna_dat.columns.tolist()
    new_drop_columns = []
    for column in drop_columns:
        if column not in column_list:
            continue
        else:
            new_drop_columns.append(column)

    df_rna_dat = df_rna_dat.drop(columns=new_drop_columns)

    utils.save_df(save_path, df_rna_dat)
    return df_rna_dat


def columns_process(file_name_id, save_dir, biocyc_gene_data_dir, biocyc_rna_data_dir):
    """
    :param biocyc_data_dir:
    :param file_name_id: Lishan id
    :return:
    """
    path_rna_dat = biocyc_rna_data_dir + 'rnas.dat.tsv'
    path_gene_rna = biocyc_gene_data_dir + f'{file_name_id}_RNA_gene_bio.tsv'
    if os.path.exists(path_rna_dat):
        df_rna_dat = utils.load_df(path_rna_dat)
    else:
        rna_dat_columns = [
            'UNIQUE-ID',
            'TYPES',
            'COMMON-NAME',
            'INSTANCE-NAME-TEMPLATE',
            'UNMODIFIED-FORM',
            'ANTICODON',
            'COMMENT',
            'GENE',
            'MODIFIED-FORM',
            'SYNONYMS',
        ]
        df_rna_dat = pd.DataFrame(columns=rna_dat_columns)
    if os.path.exists(path_gene_rna):
        df_gene_rna = utils.load_df(path_gene_rna)
    else:
        gene_rna_columns = [
            'Gene_biocyc_ID',
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
            'END-BASE',
        ]
        df_gene_rna = pd.DataFrame(columns=gene_rna_columns)

    # Initialize storage path

    # Ensure fields exist
    column_list = df_rna_dat.columns.tolist()
    if 'UNMODIFIED-FORM' not in column_list:
        df_rna_dat['UNMODIFIED-FORM'] = np.nan

    # Process rna.dat column names
    save_path_rna_dat = save_dir + 'rnas.dat_bio.tsv'
    df_rna_dat = drop_rna_dat_columns(df_rna_dat, save_path_rna_dat)

    # Match rna.dat's GENE column with gene_of_rna's Gene_biocyc_ID column, simultaneously modify duplicate column name issue
    merge_df, df_rna_dat, df_gene_rna = match_by_GENE(df_rna_dat, df_gene_rna)

    # Perform second match
    # Add following code in rna_sop_1st_process function
    merge_df['UNMODIFIED-FORM'] = merge_df['UNMODIFIED-FORM'].astype(str)  # Convert to string
    df_gene_rna['Gene_biocyc_ID'] = df_gene_rna['Gene_biocyc_ID'].astype(str)  # Convert to string
    # Then call merge function
    final_df, fail_second_time = match_by_UNMODIFIED_FORM(
        merge_df,
        rna_dat_df=df_rna_dat,
        gene_rna_df=df_gene_rna
    )

    save_merge_res_path = save_dir + file_name_id + '_RNA_entity_bio.tsv'
    utils.save_df(save_merge_res_path, final_df)

    save_fail_second_time_path = save_dir + file_name_id + '_rnas.dat_bio_fail_v0.tsv'
    utils.save_df(save_fail_second_time_path, fail_second_time)

    # Handle multi-value issue
    df = deal_multiply_data(save_merge_res_path)
    df_fail = deal_multiply_data(save_fail_second_time_path)

    return df
