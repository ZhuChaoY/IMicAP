import shutil
import utils.my_df_function as utils


def prepare_biocyc_gene(li_shan_id, biocyc_dir, save_dir):
    """
    :param file_name_save_dir:
    :param biocyc_dir:
    :param now_time: Processing time for this run
    :param li_shan_id: Lishan ID
    :param biocyc_res_time: Generation time of Biocyc data processing results
    :return:
    """
    # Store Biocyc tables prepared for Gene SOP processing
    utils.create_dir(save_dir)

    data_path = biocyc_dir + li_shan_id + '/Gene/Biocyc_gene_3rd_result/' \
                + li_shan_id + '_gene_bio.tsv'
    save_path = save_dir + li_shan_id + '_gene_bio.tsv'
    shutil.copy(data_path, save_path)

    data_path = biocyc_dir + li_shan_id + '/Gene/Biocyc_gene_3rd_result/' \
                + li_shan_id + '_RNA_gene_bio.tsv'
    save_path = save_dir + li_shan_id + '_RNA_gene_bio.tsv'
    shutil.copy(data_path, save_path)


def filter_ncbi_info(df):
    # print(df['type_of_gene'].unique().tolist())
    df_gene_info = df[df['type_of_gene'].isin(['protein-coding', 'pseudo'])]
    df_rna_info = df[df['type_of_gene'].isin(['tRNA', 'rRNA', 'ncRNA', 'miscRNA'])]

    return df_gene_info, df_rna_info


# Get tax_id for each data entry
def get_target_tax_id(ncbi_target_tax_id_dict):
    tax_ids = [
        'substrains_tax_id',
        'strains_tax_id',
        'serotype_tax_id',
        'subspecies_tax_id',
        'species_tax_id'
    ]

    list_tax_id = []
    for tax_id in tax_ids:
        tax_id_value = ncbi_target_tax_id_dict.get(tax_id)
        if ncbi_target_tax_id_dict.get(tax_id) != 'NA_NO':
            list_tax_id.append(tax_id_value)
    return list_tax_id


def prepare_ncbi_gene(li_shan_id, ncbi_data_path, ncbi_target_tax_id_dict, save_dir):
    df = utils.load_df(ncbi_data_path)

    # First filter NCBI data by specified target_tax_id_list
    target_tax_id = get_target_tax_id(ncbi_target_tax_id_dict)
    print(target_tax_id)
    df = df[df['tax_id'].isin(target_tax_id)]

    # Then filter out gene and rna data separately
    df_gene, df_rna = filter_ncbi_info(df)

    utils.create_dir(save_dir)

    # Store
    path_save_gene = save_dir + li_shan_id + '_gene_ncbi.tsv'
    path_save_rna = save_dir + li_shan_id + '_RNA_ncbi.tsv'
    utils.save_df(path_save_gene, df_gene)
    utils.save_df(path_save_rna, df_rna)

    count_d_list = [
        {
            'file_name': li_shan_id + '_gene_ncbi.tsv',
            'file_count': len(df_gene)
        },
        {
            'file_name': li_shan_id + '_RNA_ncbi.tsv',
            'file_count': len(df_rna)
        }
    ]
    file_count_dict_list = count_d_list

    return df_gene, len(df_gene), file_count_dict_list
