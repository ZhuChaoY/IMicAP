"""
Editor: INK
Create Time: 2024/6/4 10:09
File Name: change_columns.py
Function: 
"""
import utils.my_df_function as until


# Renaming process
def rename_process(df):
    rename_dict = {'tax_id': 'strains_tax_id',
                   'geneid': 'NCBI_gene_id',
                   'locustag': 'locus_tag'}

    column_list = df.columns.tolist()
    temp_dict = {}
    for column in rename_dict:
        if column in column_list:
            new_column = rename_dict.get(column)
            d = {column: new_column}
            temp_dict.update(d)

    df = df.rename(columns=temp_dict)
    return df


# Delete column names process
def drop_process(df):
    drop_list = ['dbxrefs',
                 'description',
                 'other_designations',
                 'UniProt_id',
                 'product_name',
                 'gene_name',
                 'accession_id',
                 'product']
    column_list = df.columns.tolist()
    remove_list = []
    for column in drop_list:
        if column not in column_list:
            remove_list.append(column)

    for column in remove_list:
        drop_list.remove(column)
    # print(column_list)
    print('Deleted columns as follows:')
    print(drop_list)
    df = df.drop(columns=drop_list)
    return df
