import gzip
import os

import pandas as pd

import utils.my_df_function as utils


def save_chunk(file_count, save_dir, columns_str, line_list):
    file_name = f'chunk_{file_count}.tsv'
    save_path = os.path.join(save_dir, file_name)
    print(file_name)
    with open(save_path, 'w+', encoding='utf-8') as f1:
        f1.write(columns_str)
        f1.writelines(line_list)


class TaxonGeneFilter:
    def __init__(self, path_csn, input_path, out_dir, chunk_size):

        self.path_csn = path_csn
        self.input_path = input_path
        self.output_dir = out_dir
        self.chunk_size = chunk_size
        self.chunks_dir = os.path.join(self.output_dir, 'chunks')

    # Store slice
    def split_chunks(self):
        columns_str = 'tax_id	GeneID	Symbol	LocusTag	Synonyms	dbXrefs	chromosome	map_location	description	' \
                      'type_of_gene	Symbol_from_nomenclature_authority	Full_name_from_nomenclature_authority	' \
                      'Nomenclature_status	Other_designations	Modification_date	Feature_type\n'

        data_path = self.input_path
        save_dir = self.chunks_dir
        utils.create_dir(save_dir)

        is_first_line = True
        apart_count = 0
        file_count = 0
        line_list = []
        with gzip.open(data_path, 'rt', encoding='utf-8') as f:
            for line in f:

                # Skip first line column header
                if is_first_line:
                    is_first_line = False
                    continue

                # Every twenty thousand as a slice
                if apart_count == self.chunk_size:
                    # store chunk
                    save_chunk(
                        file_count=file_count,
                        save_dir=save_dir,
                        columns_str=columns_str,
                        line_list=line_list
                    )
                    # Initialize count
                    file_count += 1
                    apart_count = 0
                    line_list = []
                else:
                    apart_count = apart_count + 1
                    line_list.append(line)

        if len(line_list) > 0:
            save_chunk(file_count=file_count,save_dir=save_dir,columns_str=columns_str,line_list=line_list)

    def filter_process(self):
        path_csn = self.path_csn
        chunk_data_dir = self.chunks_dir
        chunk_file_list = os.listdir(chunk_data_dir)

        df_csn = utils.load_df(path_csn)
        tax_id_list = df_csn['tax_id'].tolist()

        final_df_list = []

        for chunk_file in chunk_file_list:
            data_path = os.path.join(chunk_data_dir, chunk_file)
            df_chunk = pd.read_csv(data_path, sep='\t', dtype=str, index_col=False, na_values='-').fillna('NA_NO')

            df_chunk = df_chunk[df_chunk['tax_id'].isin(tax_id_list)]
            if len(df_chunk) > 0:
                final_df_list.append(df_chunk)
                print(f'##{chunk_file}##')
                print(len(df_chunk))

        df_final = pd.concat(final_df_list)
        rename_dict = {
            '#tax_id': 'tax_id'
        }
        df_final = df_final.rename(columns=rename_dict)

        save_dir = os.path.join(self.output_dir, 'Data')
        utils.create_dir(save_dir)

        file_name = f'ncbi_info_filter_result.tsv'
        utils.save_df(os.path.join(save_dir, file_name), df_final)

        record_dir = os.path.join(self.output_dir, 'Record')
        utils.create_dir(record_dir)
        record_file_name = f'Total {len(df_final)} records meeting requirements'
        with open(os.path.join(record_dir, record_file_name), 'w+'):
            pass

        return df_final

    def run(self):
        self.split_chunks()
        df_final = self.filter_process()
        return df_final
