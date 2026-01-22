import utils.my_df_function as utils


def harmonize_attributes(file_name_id, save_dir, df):
    # Create folder
    utils.create_dir(save_dir)
    df_copy = df.copy()
    # Current df column set
    already_has_columns = df_copy.columns.tolist()

    # Initialize columns not existing in current df
    column_not_exist = []

    # Keep only following column names
    target_columns = ['strains_tax_id',
                      'NCBI_gene_id',
                      'gene_biocyc_id',
                      'dbxrefs',
                      'description',
                      'gene_name',
                      'accession_id',
                      'product',
                      'symbol',
                      'synonyms',
                      'description',
                      'other_designations',
                      'type_of_gene',
                      'UniProt-id',
                      'product_name',
                      'types',
                      'instance_name_template',
                      'dblinks',
                      'abbrev_name',
                      'locus_tag',
                      'source_ortholog',
                      'chromosome',
                      'symbol_from_nomenclature_authority',
                      'full_name_from_nomenclature_authority',
                      'start-base',
                      'end-base',
                      'left-end-position',
                      'right-end-position',
                      'transcription-direction',
                      ]
    for column in target_columns:
        if column not in already_has_columns:
            column_not_exist.append(column)

    # Supplement non-existing columns
    for column in column_not_exist:
        df_copy[column] = 'NA_NO'

    # Fill empty values
    df_copy = df_copy.fillna('NA_NO')
    df_copy = df_copy[target_columns]

    save_path = save_dir + file_name_id + '_gene2protein_v1.tsv'
    utils.save_df(save_path, df_copy)
