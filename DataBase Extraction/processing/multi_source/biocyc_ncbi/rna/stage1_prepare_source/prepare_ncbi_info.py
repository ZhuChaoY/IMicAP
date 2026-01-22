import os
import utils.my_df_function as utils


def filter_ncbi_info(df):
    df_gene_info = df[df['type_of_gene'].isin(['protein-coding', 'pseudo'])]
    df_rna_info = df[df['type_of_gene'].isin(['tRNA', 'rRNA', 'ncRNA', 'miscRNA'])]

    return df_gene_info, df_rna_info


# Split
def split_ncbi_info(data_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)  # Ensure output directory exists

    chunk_size = 20000

    with open(data_path, 'r') as f:
        header = f.readline()  # Read title line (including starting # and newline)

        chunk_number = 1
        current_chunk = []
        line_count = 0

        for line in f:  # Continue reading remaining lines
            current_chunk.append(line)
            line_count += 1

            if line_count == chunk_size:
                # Write current chunk
                output_file = os.path.join(output_dir, f'chunk_{chunk_number}.tsv')
                with open(output_file, 'w') as out_f:
                    out_f.write(header)  # Write title
                    out_f.writelines(current_chunk)  # Write data

                chunk_number += 1
                current_chunk = []
                line_count = 0

        # Process remaining data less than 20000 lines at end
        if current_chunk:
            output_file = os.path.join(output_dir, f'chunk_{chunk_number}.tsv')
            with open(output_file, 'w') as out_f:
                out_f.write(header)
                out_f.writelines(current_chunk)

    print(f"File has been split into {chunk_number} chunks, saved in directory {output_dir}")


# Filter
# Get data where ID is not NA_NO for subsequent filtering
def get_target_tax_id(ncbi_target_tax_id_dict):
    tax_ids_types = [
        'substrains_tax_id',
        'strains_tax_id',
        'serotype_tax_id',
        'subspecies_tax_id',
        'species_tax_id'
    ]

    final_tax_id_list = []

    for tax_id in tax_ids_types:
        id_value = ncbi_target_tax_id_dict.get(tax_id)
        if id_value != 'NA_NO':
            final_tax_id_list.append(id_value)
    return final_tax_id_list


def prepare_ncbi_data(li_shan_id, ncbi_data_path, ncbi_target_tax_id_dict, save_dir):
    df = utils.load_df(ncbi_data_path)

    # 00) First filter NCBI data by tax_id
    target_tax_id_list = get_target_tax_id(ncbi_target_tax_id_dict)
    df = df[df['tax_id'].isin(target_tax_id_list)]

    # 01) Then filter out rna data with type [tRNA, rRNA, ncRNA or miscRNA]
    df_gene, df_rna = filter_ncbi_info(df)

    save_dir = save_dir + 'NCBI_info/'
    utils.create_dir(save_dir)

    # 02) Store
    path_save_rna = save_dir + li_shan_id + '_RNA_ncbi.tsv'
    if len(df_rna) > 0:
        utils.save_df(path_save_rna, df_rna)
        return True, df_rna
    else:
        print('No qualified NCBI info data exists')
        with open(save_dir + 'No eligible NCBI-INFO data found..txt', 'w+'):
            pass
        utils.save_df(path_save_rna, df_rna)
        return False, df_rna

