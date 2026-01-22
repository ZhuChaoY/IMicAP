import pandas as pd
import utils.my_df_function as utils


# First letter lowercase
def first_letter_lowercase(s):
    return s[0].lower() + s[1:]


# Merge execution function
def combine_function(dblink_rna, dblink_gene):
    # If one of two is empty, select non-empty value as result to return
    if pd.isnull(dblink_gene):
        return dblink_rna
    if pd.isnull(dblink_rna):
        return dblink_gene

    # If both not empty, convert to list for next step merge
    list_dblink_rna = utils.transform_back_to_list(dblink_rna)
    list_dblink_gene = utils.transform_back_to_list(dblink_gene)

    # Supplement RNA elements from dblink_rna into dblink_gene
    for item in list_dblink_rna:
        if 'RNA' in item:
            list_dblink_gene.append(item)
            print('Successfully supplemented ' + item + ' attached with RNA into DBLNKS_gene')
        else:
            new_item = first_letter_lowercase(item)
            list_dblink_gene.append(new_item)
        list_dblink_gene = list(set(list_dblink_gene))

    # Convert to specified multi-value form
    str_dblink_gene = utils.change_list_to_special_data(list_dblink_gene)
    return str_dblink_gene


# Start DBLINK merge process
def dblink_combine_process(df):
    # Determine whether merge processing is needed
    columns_list = df.columns.tolist()

    # Only merge if both exist, otherwise directly end processing
    if 'DBLINKS_RNA' not in columns_list or 'DBLINKS_gene' not in columns_list:
        return df

    # Both fields exist, merge
    df['DBLINKS_gene'] = df.apply(lambda x: combine_function(x['DBLINKS_RNA'], x['DBLINKS_gene']), axis=1)

    # Delete specified fields
    drop_columns = ['DBLINKS_RNA']
    df = df.drop(columns=drop_columns)
    return df


def merge_process(df, file_name_id, save_dir):
    # Initialize storage path
    utils.create_dir(save_dir)

    # Merge possible existing DBLINK fields
    df = dblink_combine_process(df)

    save_path = save_dir + file_name_id + '_RNA_entity_bio.tsv'
    utils.save_df(save_path, df)

    return df
