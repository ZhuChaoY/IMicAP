import json
import os
import pandas as pd
import utils.my_df_function as utils


class RNANormaliztion:

    def __init__(self):

        self.RNA_main_table_dir = 'F:/Fojann_batch3_20250708/04_ads/PROC_SOP/RNA_RefSeq/RefSeq_RNA_SOP_Result_20250801/3rd_RefSeq_RNA_clean_v2_res_20250801/Kaifa/RNA_Maintable_submit_20250801/'
        self.data_dir_1st = f'F:/Fojann_batch3_20250708/02_dwd/DB_NCBI/NCBI_fna_sequence/NCBI_RNA_fna_sequence_1st_filter_{self.deal_time_1st}/NCBI_RNA_locus_tag/'
        self.output_dir = f'D:/MyCode/Code/CodeForPaper/result/single_source/DB_ncbi/ncbi_RNA_fna/'

        self._ini_directory()
        self.record_list = []

    def _ini_directory(self):
        """
        Initialize directories
        :return:
        """
        utils.create_dir(self.output_dir + 'Data/')
        utils.create_dir(self.output_dir + 'Record/')
        utils.create_dir(self.output_dir + 'Source/')
        utils.create_dir(self.output_dir + 'Check/')

    def _save_record(self):
        save_path = self.output_dir + 'Record/record.tsv'
        df_record = pd.DataFrame(self.record_list)
        utils.save_df(save_path, df_record)

    def step1_build_mapping_dict(self):
        """
        Build comparison dictionary for matching use
        :return:
        """

        # 1. Initialize mapping_dict
        mapping_dict = {}

        # 2. Initialize file collection and corresponding main_table path collection
        sequence_file_list = os.listdir(self.data_dir_1st)
        main_table_path_list = utils.get_file_path_list(self.RNA_main_table_dir, [])

        # 3. Build mapping_dict
        for seq_file in sequence_file_list:
            if not seq_file.endswith('.tsv'):
                continue

            seq_file_name_id = seq_file.replace(f'_NCBI_RNA_locus_tag_{self.deal_time_1st}.tsv', '')
            mapping_path_list = []

            for path in main_table_path_list:

                mapping_str = seq_file_name_id + '_'
                if mapping_str in path:
                    mapping_path_list.append(path)
            mapping_dict[seq_file] = mapping_path_list
            if len(mapping_path_list) == 0:
                print(seq_file)
                print(mapping_path_list)

        # 4. Store
        save_dir = self.output_dir + 'Check/File_mapping_dict/'
        utils.create_dir(save_dir)
        save_name = 'file_mapping_dict.json'
        with open(save_dir + save_name, 'w+') as f:
            json.dump(mapping_dict, f, indent=4)

        return mapping_dict

    def step2_append_columns(self, mapping_dict):

        for seq_file in mapping_dict:
            # 1. Initialize data
            main_path_list = mapping_dict.get(seq_file)

            seq_path = self.data_dir_1st + seq_file
            df_seq = utils.load_df(seq_path)
            d = {
                'file_name': seq_file,
                'file_count': len(df_seq)
            }
            self.record_list.append(d)

            list_df_main_table = []
            for main_path in main_path_list:
                # print(main_path)
                temp_df = utils.load_df(main_path)
                list_df_main_table.append(temp_df)

                temp_list = main_path.split('/')
                d = {
                    'file_name': temp_list[-1],
                    'file_count': len(temp_df)
                }
                self.record_list.append(d)

            df_main_table = pd.concat(list_df_main_table)
            target_columns = ['locus_tag', 'RNA_name', 'RNA_lishan_id']
            df_main_table = df_main_table[target_columns]

            # 2. Modify column names
            rename_dict = {
                'product': 'RNA_name'
            }
            df_seq = df_seq.rename(columns=rename_dict)

            # 3. Match add columns
            merged_df = pd.merge(
                df_seq,
                df_main_table,
                on=['locus_tag', 'RNA_name'],
                how='left'
            )

            # 4. Filter out match failed and match successful
            merged_success = merged_df[merged_df['RNA_lishan_id'].notna()]
            merged_fail = merged_df[merged_df['RNA_lishan_id'].isnull()]

            # 5. Store
            save_dir = self.output_dir + f'Data/NCBI_RNA_locus_tag_{self.now_time}/'
            utils.create_dir(save_dir)
            save_name = seq_file.replace(self.deal_time_1st, self.now_time)
            utils.save_df(save_dir + save_name, merged_success)
            d = {
                'file_name': save_name,
                'file_count': len(merged_success)
            }
            self.record_list.append(d)

            if len(merged_fail) > 0:
                save_dir_fail = self.output_dir + f'Check/NCBI_RNA_locus_tag_fail/'
                utils.create_dir(save_dir_fail)
                save_name = seq_file.replace(self.deal_time_1st, self.now_time)
                save_name = 'Fail_' + save_name
                utils.save_df(save_dir_fail + save_name, merged_fail)
                d = {
                    'file_name': save_name,
                    'file_count': len(merged_fail)
                }
                self.record_list.append(d)

    def process_all(self):
        print('step1: Build mapping_dict')
        mapping_dict = self.step1_build_mapping_dict()

        print('step2: Match add columns')
        self.step2_append_columns(mapping_dict)

        self._save_record()


if __name__ == '__main__':
    cleaner = RNANormaliztion()
    cleaner.process_all()
