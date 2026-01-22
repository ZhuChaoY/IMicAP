import os
import numpy as np
import pandas as pd
import utils.my_df_function as utils


def start_combine_synonyms(df):
    # Convert data to original type
    df['SYNONYMS'] = df['SYNONYMS'].apply(lambda x: utils.transform_back_to_list(x))
    df['Synonyms'] = df['Synonyms'].apply(lambda x: utils.transform_back_to_list(x))

    # print(df)

    # Specific merge process
    def combine_process(synonyms_gene, synonyms_rna, gene_name):

        # Exclude special situations during merge process
        # print(synonyms_gene)
        if type(synonyms_gene) != list and pd.isnull(synonyms_gene):
            final_res = synonyms_rna
        elif type(synonyms_rna) != list and pd.isnull(synonyms_rna):
            final_res = synonyms_gene
        else:
            if type(synonyms_gene) == list:
                list_synonyms_gene = synonyms_gene
            else:
                list_synonyms_gene = [synonyms_gene]

            if type(synonyms_rna) == list:
                list_synonyms_rna = synonyms_rna
            else:
                list_synonyms_rna = [synonyms_rna]

            final_res = list(set(list_synonyms_rna + list_synonyms_gene))

        # If final result is empty no need to compare
        if type(final_res) != list and pd.isnull(final_res):
            return final_res

        # If single element str, convert to list for convenient comparison
        if type(final_res) == list:
            list_final_res = final_res
        else:
            list_final_res = [final_res]

        # If gene_name exists in merge result, delete it
        if gene_name in list_final_res:
            list_final_res.remove(gene_name)

        # If empty after deletion, directly return empty
        if len(list_final_res) == 0:
            return np.nan

        final_str = utils.change_list_to_special_data(list_final_res)
        return final_str

    df['Synonyms'] = df.apply(
        lambda x: combine_process(x['Synonyms'], x['SYNONYMS'], x['gene_name']), axis=1
    )

    # Delete column names
    drop_column = ['SYNONYMS']
    df = df.drop(columns=drop_column)

    return df


# Data comparison
def is_all_the_same(rna_column, gene_column, df, save_dir):
    # Find rows that are different
    differences = df[df[rna_column] != df[gene_column]]

    if not differences.empty:
        print("Values differ in following rows:")
        column_list = ['UNIQUE-ID', rna_column, gene_column]
        differences = differences[column_list]
        save_path = save_dir + rna_column + '_Different_' + gene_column + '.tsv'
        utils.save_df(save_path, differences)
        # print(differences)
        return False
    else:
        print("Two columns have completely identical values.")
        return True


# Process data based on comparison result
def start_compare_process(df, path_save_compare_result, save_dir):
    # Column name pairs to compare
    special_column_couple = {
        'DBLINKS_RNA': ' DBLINKS_gene',
        'COMMENT_RNA': 'COMMENT_gene'
    }

    column_list = df.columns.tolist()

    # Delete previously generated results
    if os.path.exists(path_save_compare_result):
        os.remove(path_save_compare_result)

    # Compare sequentially
    for rna_column in special_column_couple:
        gene_column = special_column_couple.get(rna_column)

        with open(path_save_compare_result, 'a+') as f:
            f.write(rna_column + 'and' + gene_column + ': ')

            # Compare only if both columns exist
            if rna_column in column_list and gene_column in column_list:
                # Determine if identical
                is_same = is_all_the_same(rna_column=rna_column, gene_column=gene_column, df=df, save_dir=save_dir)

                # If completely identical, delete column from gene
                if is_same:
                    drop_column = [gene_column]
                    df = df.drop(columns=drop_column)
                    f.write('Same, delete ' + gene_column + '\n')
                else:
                    f.write('Different, keep both columns\n')
            else:
                f.write('At least one column, this table does not have\n')
    return df


def rename_and_drop_process(df):
    column_list = df.columns.tolist()

    drop_column_list = ['CGSC-ID', 'GENE']

    new_drop_columns = []
    for column in drop_column_list:
        if column in column_list:
            new_drop_columns.append(column)

    drop_column_list = new_drop_columns
    df = df.drop(columns=drop_column_list)
    return df


def synonym_normalize(df, file_name_id, save_dir):
    utils.create_dir(save_dir)

    # Delete or rename column names
    df = rename_and_drop_process(df)

    # Compare data and generate comparison situation
    path_save_compare_res = save_dir + 'columns compare result.txt'
    df = start_compare_process(df, path_save_compare_res, save_dir)

    if 'SYNONYMS' not in df.columns:
        df['SYNONYMS'] = np.nan
    # Merge Synonym, SYNONYMS, gene_name
    df = start_combine_synonyms(df)

    # Storage path
    save_path = save_dir + file_name_id + '_RNA_entity_bio.tsv'
    utils.save_df(save_path, df)

    return df
