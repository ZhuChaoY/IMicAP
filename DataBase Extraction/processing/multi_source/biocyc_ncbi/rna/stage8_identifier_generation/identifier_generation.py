import os
import pandas as pd
import utils.my_df_function as utils


# Combine lishan_csn_id to construct RNAm_????_
def build_rna_lishan_id_process(df, sub_id_index, batch_num):
    # 00) Get primary key subscript
    last_index = sub_id_index

    # 01) Build temporary column based on previous subtable subscript count, for primary key addition
    df['temp_count'] = range(last_index, last_index + len(df))

    def build_process(temp_count, batch_count):
        sub_id = f'RNAm_{batch_count}_{temp_count}'
        return sub_id

    # 02) Construct subtable primary key ID
    sub_id_col = 'RNA_subtable_id'
    df[sub_id_col] = df['temp_count'].apply(lambda x: build_process(temp_count=x, batch_count=batch_num))
    drop_column = ['temp_count']
    df = df.drop(columns=drop_column)

    # 03) Update primary key subscript
    last_index = last_index + len(df)

    return df, last_index


# Column name addition process
# Add current_name etc. data
def append_data(row, ref_row, append_column_list, current_name_column):
    # Assign values to specified columns that meet conditions
    for append_column in append_column_list:
        if append_column == 'species_name':
            row[append_column] = ref_row['species']
        else:
            new_value = ref_row[append_column]
            row[append_column] = new_value
    row['current_scientific_name'] = ref_row[current_name_column]

    # Other columns not meeting conditions, set to NA_NO
    all_append_column_list = ['species_name',
                              'species_tax_id',
                              'lishan_species_tax_id',

                              'strains_name',
                              'strains_tax_id',
                              'lishan_strains_tax_id',

                              'subspecies_name',
                              'subspecies_tax_id',
                              'lishan_subspecies_tax_id',

                              'substrains_name',
                              'substrains_tax_id',
                              'lishan_substrains_tax_id',

                              'serotype_name',
                              'serotype_tax_id',
                              'lishan_serotype_tax_id', ]
    for column in all_append_column_list:
        if column not in append_column_list:
            row[column] = 'NA_NO'
    return row


# Process each row of data sequentially
def append_target_column(row, ref_row):
    strains_tax_id_gene = row['strains_tax_id']
    gene_biocyc_id = row['gene_biocyc_id']
    unique_id = row['unique_id']

    # All column names to be added
    all_append_column_list = ['species_name',
                              'species_tax_id',
                              'lishan_species_tax_id',

                              'strains_name',
                              'strains_tax_id',
                              'lishan_strains_tax_id',

                              'subspecies_name',
                              'subspecies_tax_id',
                              'lishan_subspecies_tax_id',

                              'substrains_name',
                              'substrains_tax_id',
                              'lishan_substrains_tax_id',

                              'serotype_name',
                              'serotype_tax_id',
                              'lishan_serotype_tax_id', ]

    if gene_biocyc_id != 'NA_NO' or unique_id != 'NA_NO':
        row['current_scientific_name'] = ref_row.get('current_scientific_name')
        row = append_data(row, ref_row, all_append_column_list, current_name_column='current_scientific_name')
    else:
        sub_strains_tax_id_ref = ref_row['substrains_tax_id']
        strains_tax_id_ref = ref_row['strains_tax_id']
        sero_type_tax_id_ref = ref_row['serotype_tax_id']
        sub_species_tax_id_ref = ref_row['subspecies_tax_id']
        species_tax_id_ref = ref_row['species_tax_id']

        # Determine which class of data in Ref table matches Protein table's id, add data for corresponding class

        if strains_tax_id_gene == sub_strains_tax_id_ref:
            column_name_column = 'substrains_name'
            append_column_list = ['species_name',
                                  'species_tax_id',
                                  'lishan_species_tax_id',

                                  'strains_name',
                                  'strains_tax_id',
                                  'lishan_strains_tax_id',

                                  'subspecies_name',
                                  'subspecies_tax_id',
                                  'lishan_subspecies_tax_id',

                                  'substrains_name',
                                  'substrains_tax_id',
                                  'lishan_substrains_tax_id',

                                  'serotype_name',
                                  'serotype_tax_id',
                                  'lishan_serotype_tax_id',
                                  ]
            row = append_data(row=row, append_column_list=append_column_list, ref_row=ref_row,
                              current_name_column=column_name_column)
        elif strains_tax_id_gene == strains_tax_id_ref:
            append_column_list = ['species_name',
                                  'species_tax_id',
                                  'lishan_species_tax_id',

                                  'strains_name',
                                  'strains_tax_id',
                                  'lishan_strains_tax_id',

                                  'subspecies_name',
                                  'subspecies_tax_id',
                                  'lishan_subspecies_tax_id',

                                  'serotype_name',
                                  'serotype_tax_id',
                                  'lishan_serotype_tax_id',
                                  ]
            column_name_column = 'strains_name'
            row = append_data(row=row, append_column_list=append_column_list, ref_row=ref_row,
                              current_name_column=column_name_column)
        elif strains_tax_id_gene == sero_type_tax_id_ref:
            append_column_list = ['species_name',
                                  'species_tax_id',
                                  'lishan_species_tax_id',

                                  'subspecies_name',
                                  'subspecies_tax_id',
                                  'lishan_subspecies_tax_id',

                                  'serotype_name',
                                  'serotype_tax_id',
                                  'lishan_serotype_tax_id',
                                  ]
            column_name_column = 'serotype_name'
            row = append_data(row=row, append_column_list=append_column_list, ref_row=ref_row,
                              current_name_column=column_name_column)
        elif strains_tax_id_gene == sub_species_tax_id_ref:
            # print(strains_tax_id_gene)
            append_column_list = ['species_name',
                                  'species_tax_id',
                                  'lishan_species_tax_id',

                                  'subspecies_name',
                                  'subspecies_tax_id',
                                  'lishan_subspecies_tax_id',
                                  ]
            column_name_column = 'subspecies_name'
            row = append_data(row=row, append_column_list=append_column_list, ref_row=ref_row,
                              current_name_column=column_name_column)
        elif strains_tax_id_gene == species_tax_id_ref:

            append_column_list = ['species_name',
                                  'species_tax_id',
                                  'lishan_species_tax_id',
                                  ]
            column_name_column = 'species'
            row = append_data(row=row, append_column_list=append_column_list, ref_row=ref_row,
                              current_name_column=column_name_column)
        else:
            with open('./RNA_7th_strains_tax_id_ERROR.txt', 'a+') as f:
                f.write(strains_tax_id_gene + 'Protein table strains_tax_id cannot match any class in Ref table!!!\n')
            print('Protein table strains_tax_id cannot match any class in Ref table!!!')

    return row


# Add specified columns to RNA table
def generate_process(
        batch_num,
        file_name_id,
        df_csn,
        save_dir,
        ref_row,
        sub_id_index,
        data_path
):
    # Columns to add
    append_column_list = [
        'species_name',
        'species_tax_id',
        'lishan_species_tax_id',
        'strains_name',
        'strains_tax_id',
        'lishan_strains_tax_id',
        'subspecies_name',
        'subspecies_tax_id',
        'lishan_subspecies_tax_id',
        'substrains_name',
        'substrains_tax_id',
        'lishan_substrains_tax_id',
        'serotype_name',
        'serotype_tax_id',
        'lishan_serotype_tax_id'
    ]

    csn_target_column = ['lishan_csn_id', 'current_scientific_name']
    df_csn = df_csn[csn_target_column]

    # Construct storage path
    utils.create_dir(save_dir)

    # Columns to delete
    drop_columns = ['lower_gene_biocyc_id', 'lower_accession_id']

    # File path for adding columns

    # Construct storage path
    save_path = save_dir + file_name_id + '_RNA_entity_bioncbi_v0_mapping.tsv'

    # Record processing situation
    path_deal_situation = save_dir + 'Status of data processing.txt'
    with open(path_deal_situation, 'w+'):
        pass

    # Avoid data_path corresponding data being empty, only column headers
    try:
        df = utils.load_df(data_path)

        # Delete unnecessary columns
        for column in drop_columns:
            if column in df.columns:
                df = df.drop(columns=column)

        # Add data values sequentially
        df = df.apply(append_target_column, args=[ref_row, ], axis=1)

        # Get 'lishan_csn_id' via 'current_scientific_name'
        df = pd.merge(df, df_csn,
                      on='current_scientific_name', how='left')

        # Construct RNA_subtable_id via lishan_csn_id
        df, sub_id_index = build_rna_lishan_id_process(df, sub_id_index, batch_num)

        utils.save_df(save_path, df)


    # Loading data_path failed, probably empty df, only column headers
    except Exception as e:
        print(e.args)
        # Record processing situation
        path_deal_situation = save_dir + 'Status of data processing.txt'
        with open(path_deal_situation, 'w+') as f:
            pass

        with open(data_path, 'r') as f:
            data_content = f.read()

        # Delete unnecessary columns
        data_content_list = data_content.split('\t')
        for column in drop_columns:
            if column in data_content_list:
                data_content_list.remove(column)
        data_content = '\t'.join(data_content_list)

        # Add new columns
        str_append_column = '\t'.join(append_column_list)
        data_content = data_content + '\t' + str_append_column

        with open(save_path, 'w+', encoding='utf-8') as f:
            f.write(data_content)

    return sub_id_index, save_path
