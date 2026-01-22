"""
Editor: INK
Create Time: 2024/10/23 13:36
File Name: StrainsRnaIntegrator.py
Function:
1. Merge data by Species
    1.1 Scan to obtain which file_name_id have same lishan_species_tax_id
    1.2 Remove redundancy and merge

2. Filter out duplicate data
    2.1 NCBI_gene_id, gene_biocyc_id same, and not NA_NO
    2.2 NCBI_gene_id not NA_NO and same, and gene_biocyc_id different

3. Classify data by biological taxonomy

"""
import os
import pandas as pd
import utils.my_df_function as utils


# Scan to obtain which file_name_id have same lishan_species_tax_id
def get_same_species(data_dir):
    data_dir = data_dir + 'Data/'

    # Initialize processing result
    scan_result_dict = {}

    # Scan sequentially
    file_id_list = os.listdir(data_dir)
    for file_id in file_id_list:

        temp_list = file_id.split('_')
        lishan_species_tax_id = temp_list[0] + '_' + temp_list[1]

        if lishan_species_tax_id not in scan_result_dict:
            contains_id_list = [file_id]
            d = {
                lishan_species_tax_id: contains_id_list
            }
        else:
            contains_id_list = scan_result_dict.get(lishan_species_tax_id)
            contains_id_list.append(file_id)
            d = {
                lishan_species_tax_id: contains_id_list
            }
        scan_result_dict.update(d)

    return scan_result_dict


# Merge RNA data with same species
def combine_RNA_with_same_species(
        dict_species_to_file_id,
        save_dir_main,
        strains_res_dir,
):
    """
    :param dict_species_to_file_id: Which file_name_id have same lishan_species_tax_id
    :param save_dir_main: Main storage path for data
    :param strains_res_dir: RNA processing results by strains
    :return:
    """

    strains_res_dir = strains_res_dir + 'Mapping/'

    # Initialize storage path
    save_dir = save_dir_main + 'Data/RNA_1st_result/'
    utils.create_dir(save_dir)
    path_combine_summary = save_dir + 'Status of data processing.txt'
    with open(path_combine_summary, 'w+', encoding='utf-8'):
        pass

    for lishan_species in dict_species_to_file_id:

        # test_id = ['lishan_670']
        # if 'lishan_species' not in test_id:
        #     continue

        # Get set of file_id to merge
        file_id_list = dict_species_to_file_id.get(lishan_species)
        with open(path_combine_summary, 'a+', encoding='utf-8') as f_record:
            f_record.write('###############\n')
            f_record.write(lishan_species + '\n')

        # Perform merging processing
        df_combine = pd.DataFrame()  # Initialize merge result
        for file_id in file_id_list:
            # Build data path to merge
            file_name = f'{file_id}_RNA_entity_bio_ncbi.tsv'
            data_path = strains_res_dir + file_name
            if not os.path.exists(data_path):
                continue
            df = utils.load_df(data_path)

            df_combine = pd.concat([df, df_combine])
            df_combine = df_combine.fillna('NA_NO')
            df_combine = df_combine.drop_duplicates()

        # Store merge result
        if len(df_combine) > 0:
            save_name = f'{lishan_species}_RNA.tsv'
            save_path = save_dir + save_name
            utils.save_df(save_path, df_combine)


def deal_multiply_lines_process(save_dir_main):
    data_dir_1st_res = save_dir_main + 'Data/RNA_1st_result/'  # Data path for first processing result
    save_dir_2nd_res = save_dir_main + 'Data/RNA_2nd_result/'  # Storage path for second processing result
    save_dir_submit_res = save_dir_main + f'Submit/'  # Storage path for results to developers
    utils.create_dir(save_dir_2nd_res)
    utils.create_dir(save_dir_submit_res)

    # Initialize processing situation
    path_deal_summary = save_dir_2nd_res + 'Status of data processing.txt'
    with open(path_deal_summary, 'w+', encoding='utf-8'):
        pass
    path_deal_summary_submit = save_dir_submit_res + 'Status of data processing.txt'
    with open(path_deal_summary_submit, 'w+', encoding='utf-8'):
        pass

    # Extract duplicate values sequentially
    path_list = utils.get_file_path_list(data_dir_1st_res, [])
    for path in path_list:

        if 'Status of data processing' in path:
            continue

        # Get file name
        temp_list = path.split('/')
        file_name = temp_list[-1]

        # Extract file's lishan_species
        file_name_list = file_name.split('_')
        lishan_species_tax_id = file_name_list[1] + '_' + file_name_list[2]

        df = utils.load_df(path)  # 1st processing result
        with open(path_deal_summary, 'a+', encoding='utf-8') as f_record:
            f_record.write('######\n')

        # Filter condition 2.1 NCBI_gene_id, gene_biocyc_id same, and both not NA_NO
        filtered_2_1 = df[
            (df['NCBI_gene_id'] == df['gene_biocyc_id']) &
            (df['NCBI_gene_id'] != 'NA_NO') &
            (df['gene_biocyc_id'] != 'NA_NO')
            ]
        if len(filtered_2_1) > 0:
            save_name_2_1 = f'{lishan_species_tax_id}_gene_biocyc_id_same_other_diff.tsv'
            save_path_2_1 = save_dir_2nd_res + save_name_2_1
            utils.save_df(save_path_2_1, filtered_2_1)
        else:
            save_name_2_1 = f'{lishan_species_tax_id}_After deduplication, no data with identical NCBI_gene_id and gene_biocyc_id.txt'
            save_path_2_1 = save_dir_2nd_res + save_name_2_1
            with open(save_path_2_1, 'w+'):
                pass
            with open(path_deal_summary, 'a+', encoding='utf-8') as f_record:
                f_record.write(f'{lishan_species_tax_id}_After deduplication, no data with identical NCBI_gene_id and gene_biocyc_id\n')

        # Filter condition 2.2
        # First filter out records where NCBI_gene_id is not 'NA_NO'
        filtered_2_2 = df[df['NCBI_gene_id'] != 'NA_NO']
        df_keep_2_2 = df[df['NCBI_gene_id'] == 'NA_NO']  # NCBI_gene_id not NA_NO to keep in final

        # Check if rows with same NCBI_gene_id exist
        df_dup_2_2_1 = filtered_2_2[filtered_2_2.duplicated(subset='NCBI_gene_id', keep=False)]
        df_keep_2_2_1 = filtered_2_2.drop_duplicates(subset='NCBI_gene_id',
                                                     keep=False)  # NCBI_gene_id # NCBI_gene_id different to keep in final

        # Delete records where gene_biocyc_id is 'NA_NO' in duplicate data
        df_dup_2_2_1 = df_dup_2_2_1[df_dup_2_2_1['gene_biocyc_id'] != 'NA_NO']

        # Check again if rows with same NCBI_gene_id still exist
        df_dup_2_2_2 = df_dup_2_2_1[
            df_dup_2_2_1.duplicated(subset='NCBI_gene_id', keep=False)]  # NCBI_gene_id still same to keep in final

        # Get final result
        df_final = pd.concat([df_keep_2_2, df_keep_2_2_1, df_dup_2_2_2])

        # Store data
        if len(df_dup_2_2_2) > 0:
            save_name_2_2 = f'{lishan_species_tax_id}_gene_biocyc_id_different.tsv'
            save_path = save_dir_2nd_res + save_name_2_2
            utils.save_df(save_path, df_dup_2_2_2)
        else:
            save_name_2_2 = f'{lishan_species_tax_id}_After deduplication,No data where the NCBI_gene_id is identical but the gene_biocyc_id differs..txt'
            save_name_2_2 = save_dir_2nd_res + save_name_2_2
            with open(save_name_2_2, 'w+'):
                pass
            with open(path_deal_summary, 'a+', encoding='utf-8') as f_record:
                f_record.write(f'{lishan_species_tax_id}_After deduplication,No data where the NCBI_gene_id is identical but the gene_biocyc_id differs.\n')

        # Store processing result
        save_name_final = f'2nd_{lishan_species_tax_id}_RNA.tsv'
        save_final = save_dir_2nd_res + save_name_final
        utils.save_df(save_final, df_final)

        # Store results for developers
        save_name_final_submit = f'2nd_{lishan_species_tax_id}_RNA.tsv'
        save_final_submit = save_dir_submit_res + save_name_final_submit
        utils.save_df(save_final_submit, df_final)


# Classify data by biological level
def split_data_as_bio_define(save_dir_main, path_csn):
    # Path of original data to be split
    data_dir_submit_res = save_dir_main + f'Submit/'  # Storage path for results to developers
    file_list = os.listdir(data_dir_submit_res)

    # Get csn table for assisting classification
    df_csn = utils.load_df(path_csn)
    target_column = [
        'rank',
        'lishan_csn_id'
    ]
    df_csn = df_csn[target_column]

    # Initialize storage path for classification results
    save_dir_category = save_dir_main + f'Category/'
    utils.create_dir(save_dir_category)

    # Perform split processing sequentially
    for file in file_list:
        if 'Status of data processing' in file:
            continue

        df_csn_for_join = df_csn.copy()

        # Load data to process
        data_path = data_dir_submit_res + file
        df = utils.load_df(data_path)
        final_column_list = df.columns.tolist()

        # Get Species corresponding to this file
        temp_file_name_list = file.split('_')  # 2nd_lishan_562_RNA_v0_date.tsv
        lishan_species_tax_id = temp_file_name_list[1] + '_' + temp_file_name_list[2]

        # Get rank via join
        df_merge = pd.merge(
            left=df,
            right=df_csn_for_join,
            on='lishan_csn_id',
            how='left'
        )

        # Store data by rank classification
        rank_types = df_merge['rank'].unique().tolist()

        for rank_label in rank_types:
            # Filter out data for corresponding classification level
            df_category_part = df_merge[df_merge['rank'] == rank_label]
            df_category_part = df_category_part[final_column_list]

            # Initialize folder for classification results
            save_dir = save_dir_category + rank_label + '/'
            utils.create_dir(save_dir)

            # File name used during storage
            save_file_name = f'{lishan_species_tax_id}_{rank_label}_RNA.tsv'
            save_path = save_dir + save_file_name
            utils.save_df(save_path, df_category_part)


# 1. Merge data with same Species
def ini_combine_by_species(save_dir_main, strains_res_dir):
    data_dir = strains_res_dir
    dict_species_to_file_id = get_same_species(data_dir)
    combine_RNA_with_same_species(
        dict_species_to_file_id=dict_species_to_file_id,
        save_dir_main=save_dir_main,
        strains_res_dir=strains_res_dir,
    )


# 2. Filter out duplicate data
def ini_deal_multiply_line(save_dir_main):
    deal_multiply_lines_process(save_dir_main)


# 3. Classify data by biological classification
def ini_classify(save_dir_main, path_csn):
    # Classify data
    split_data_as_bio_define(save_dir_main, path_csn)


def run(output_dir, path_csn, strains_res_dir):
    output_dir = output_dir
    utils.create_dir(output_dir)

    strains_res_dir = strains_res_dir

    path_csn = path_csn

    ini_combine_by_species(output_dir, strains_res_dir)
    ini_deal_multiply_line(output_dir)
    ini_classify(output_dir, path_csn)

