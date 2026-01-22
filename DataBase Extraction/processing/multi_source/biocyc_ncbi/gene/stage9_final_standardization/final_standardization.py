import utils.my_df_function as utils
import pandas as pd


# 1. When symbol column is NA_NO, add gene_biocyc_id to symbol
def deal_symbol_data(df):
    def deal_process(symbol, gene_biocyc_id):

        if symbol == 'NA_NO':
            return gene_biocyc_id
        else:
            return symbol

    if 'symbol' not in df.columns:
        print('symbol')
    df['symbol'] = df.apply(
        lambda x: deal_process(x['symbol'], x['gene_biocyc_id']), axis=1
    )
    return df


# Delete rows that simultaneously meet following conditions
# Simultaneously satisfy NCBI_gene_id has no value and gene_biocyc_id has value
def filter_process(df):
    df_for_filter = df.copy()

    def conclude_process(ncbi_gene_id, gene_biocyc_id):
        if pd.isnull(ncbi_gene_id) or ncbi_gene_id == 'NA_NO':
            if pd.notna(gene_biocyc_id) and gene_biocyc_id != 'NA_NO':
                return False

        return True

    df_for_filter['is_keep'] = df_for_filter.apply(lambda x: conclude_process(x['NCBI_gene_id'], x['gene_biocyc_id']),
                                                   axis=1)
    # print(df['is_keep'])
    df_filter = df_for_filter[df_for_filter['is_keep']]

    if 'is_keep' in df_filter.columns.tolist():
        drop_column = ['is_keep']
        df_filter = df_filter.drop(columns=drop_column)

    return df_filter


# Fill type_of_gene data with type value
# When data value is "Unclassified-Genes", change to "protein-coding"
def deal_type_append(df):
    def deal_process(row):

        type_of_gene = row['type_of_gene']
        types_data = row['types']

        if type_of_gene == 'NA_NO':
            type_of_gene = types_data
        else:
            type_of_gene = type_of_gene

        if 'Unclassified-Genes' in type_of_gene:
            type_of_gene = type_of_gene.replace('Unclassified-Genes', 'protein-coding')
        # print(type_of_gene)

        row['type_of_gene'] = type_of_gene
        # print(row['type_of_gene'])
        return row

    df = df.apply(deal_process, axis=1)
    drop_column = ['types']
    df = df.drop(columns=drop_column)

    return df


def start_gene_8th_clean(df):
    df = deal_symbol_data(df)
    df = deal_type_append(df)

    return df


def finalize_output(save_dir, file_name_id, df):
    # Build 8th data storage folder
    utils.create_dir(save_dir)

    # Initialize before and after processing record
    path_dealing_8th = save_dir + 'Status of data processing.txt'
    with open(path_dealing_8th, 'w+'):
        pass

    # Build 7th data path
    df_7th_all = df.copy()

    # Perform 8th processing
    df_8th_all = start_gene_8th_clean(df_7th_all)
    df_8th_mapping = filter_process(df_8th_all)

    # Build file name
    file_mapping = file_name_id + '_gene_ncbi_biomapping.tsv'
    file_all = file_name_id + '_gene_ncbi_bio.tsv'

    # Store processing result
    save_path_mapping_check = save_dir + file_mapping
    utils.save_df(save_path_mapping_check, df_8th_mapping)
    save_path_all_data = save_dir + file_all
    utils.save_df(save_path_all_data, df_8th_all)
