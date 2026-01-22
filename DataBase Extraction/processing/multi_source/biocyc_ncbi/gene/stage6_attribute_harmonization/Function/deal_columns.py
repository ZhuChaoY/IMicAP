"""
Editor: INK
Create Time: 2024/6/3 16:27
File Name: deal_columns.py
Function: 
"""
import utils.my_df_function as until


def change_column_process(df):
    column_list = df.columns.tolist()

    # Handle column names that need modification
    rename_columns = {'tax_id': 'strains_tax_id',
                      'geneid': 'NCBI_gene_id',
                      'locustag': 'locus_tag'}
    temp_rename = {}
    for column in rename_columns:
        if column in column_list:
            new_column = rename_columns.get(column)
            d = {column: new_column}
            temp_rename.update(d)
    df = df.rename(columns=temp_rename)

    # Handle column names that need deletion
    column_list = df.columns.tolist()
    drop_columns = ['locustag',
                    'dbxrefs',
                    'chromosome',
                    'map_location',
                    'symbol_from_nomenclature_authority',
                    'full_name_from_nomenclature_authority',
                    'nomenclature_status',
                    'modification_date',
                    'feature_type',
                    'gene_name',
                    'start_base',
                    'end_base',
                    'centisome_position',
                    'component_of',
                    'left_end_position',
                    'product',
                    'right_end_position',
                    'source_ortholog',
                    'transcription_direction',
                    'replicon']
    temp_drop = []
    for column in drop_columns:
        if column in column_list:
            temp_drop.append(column)
    drop_columns = temp_drop
    print(drop_columns)

    df = df.drop(columns=drop_columns)

    columns_list = df.columns.tolist()
    if 'abbrev_name' in columns_list:
        if df['abbrev_name'].isnull().all():
            drop_columns = ['abbrev_name']
            df = df.drop(columns=drop_columns)

    return df
