import pandas as pd
import utils.my_df_function as utils


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
    # print(strains_tax_id_gene)
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

    if strains_tax_id_gene == 'NA_NO':
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
            with open('./Gene_7th_strains_tax_id_ERROR.txt', 'a+') as f:
                f.write(strains_tax_id_gene + 'Protein table strains_tax_id cannot match any class in Ref table!!!\n')
            print('Protein table strains_tax_id cannot match any class in Ref table!!!')

    return row


# Add specified columns to Gene table
def enrich_metadata(file_name_id, df_csn, save_dir, ref_row, df):
    # Columns to add
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
                          'lishan_serotype_tax_id']

    csn_target_column = ['lishan_csn_id', 'current_scientific_name']
    df_csn_copy = df_csn[csn_target_column].copy()

    # Construct storage path
    utils.create_dir(save_dir)

    # Columns to delete
    drop_columns = ['lower_gene_biocyc_id', 'lower_accession_id']

    # Construct storage path
    save_file_name_all = file_name_id + '_gene_ncbi_bio.tsv'
    save_path_all = save_dir + save_file_name_all

    df_all = df.copy()

    # Delete unnecessary columns
    for column in drop_columns:
        if column in df_all.columns:
            df_all = df_all.drop(columns=column)

    # Initialize data to add
    already_contains_column_all = df_all.columns.tolist()
    for column in append_column_list:
        if column not in already_contains_column_all:
            df_all[column] = 'NA_NO'

    # Add data values sequentially
    df_all = df_all.apply(append_target_column, args=[ref_row, ], axis=1)

    # Get 'lishan_csn_id' via 'current_scientific_name'
    df_all = pd.merge(
        df_all,
        df_csn_copy,
        on='current_scientific_name',
        how='left'
    )
    utils.save_df(save_path_all, df_all)

    return df_all