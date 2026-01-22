import os.path
import shutil

import pandas as pd

import utils.my_df_function as utils


def move_data(data_path, save_dir, file_name_id, biocyc_columns_list, file_name):
    save_path = save_dir + file_name
    if os.path.exists(data_path):
        shutil.copy(data_path, save_path)
        return True
    else:
        save_path = save_dir + f'No eligible BIOCYC data found.: {file_name}'
        print(f'No eligible BIOCYC data found.: {file_name}')
        with open(save_path, 'w+'):
            pass

        df_biocyc = pd.DataFrame(columns=biocyc_columns_list)
        utils.save_df(save_path, df_biocyc)
        return False


def deal_gene_of_biocyc(file_name_id, biocyc_gene_data_dir, biocyc_rna_data_dir, save_dir):
    """
    :param biocyc_gene_data_dir:
    :param file_name_id: Lishan ID
    :param save_dir
    :return:
    """
    biocyc_columns_list_1 = [
        'Gene_biocyc_ID',
        'UniProt-ID',
        'gene_name',
        'TYPES',
        'ACCESSION_ID',
        'CENTISOME-POSITION',
        'COMPONENT-OF',
        'INSTANCE-NAME-TEMPLATE',
        'LEFT-END-POSITION',
        'PRODUCT',
        'RIGHT-END-POSITION',
        'SOURCE-ORTHOLOG',
        'Synonyms',
        'TRANSCRIPTION-DIRECTION',
        'DBLINKS',
        'ABBREV-NAME',
        'PRODUCT-NAME',
        'REPLICON',
        'START-BASE',
        'END-BASE',
    ]
    biocyc_columns_list_2 = [
        'UNIQUE-ID',
        'TYPES',
        'COMMON-NAME',
        'GENE',
        'INSTANCE-NAME-TEMPLATE',
        'MODIFIED-FORM',
        'UNMODIFIED-FORM',
        'SYNONYMS',
        'ANTICODON',
        'COMMENT',
    ]
    # Store Biocyc tables prepared for RNA SOP processing
    save_dir = save_dir + 'Biocyc_gene/'
    utils.create_dir(save_dir)

    file_name = f'{file_name_id}_RNA_gene_bio.tsv'
    data_path = biocyc_gene_data_dir + file_name
    is_exist_1 = move_data(data_path, save_dir, file_name_id, biocyc_columns_list_1, file_name)

    file_name = f'rnas.dat.tsv'
    data_path = biocyc_rna_data_dir + file_name
    is_exist_2 = move_data(data_path, save_dir, file_name_id, biocyc_columns_list_2, file_name)

    is_exist = is_exist_1 and is_exist_2
    print(is_exist)
    return is_exist
