import os
import re
import shutil

import pandas as pd
import utils.common_data_modify as common_modify
import utils.my_df_function as until
import tarfile
import gzip


class FNAExtractor:

    # Used to determine if needed file
    @staticmethod
    def _conclude_is_need_version(data_path):
        with open(data_path, 'r') as f:
            f_content = f.read().strip('\n')
            print(f_content)
            if f_content == 'status=latest':
                is_need_file = True
            else:
                is_need_file = False
            print(is_need_file)
        return is_need_file

    # Filter correct data version folders
    @staticmethod
    def _select_right_dir_version(gcf_dir):
        gcf_version_list = os.listdir(gcf_dir)

        # When only one data version, directly use without verification
        if len(gcf_version_list) == 1:
            return gcf_version_list[0]

        # If multiple data versions, verify status files within
        for gcf_version in gcf_version_list:
            version_dir = gcf_dir + gcf_version + '/'
            path_version = version_dir + 'assembly_status.txt'
            with open(path_version, 'r') as f:
                text = f.read()
                if 'latest' in text:
                    return gcf_version

        print(gcf_version_list)
        print('Did not find version number meeting requirements!')
        return None

    # Detailed parsing process
    @staticmethod
    def _parse_process(data_path):
        # Initialize final parsing result
        final_result_list = []

        # Parse NCBI_RNA_id
        def merge_NCBI_RNA_id(data_line):
            temp_list = data_line.split(' ')
            target_data = temp_list[0]
            target_data = target_data.replace('>lcl|', '')

            return target_data

        # Parse other columns
        def merge_other_tag(data_line):
            # Use regular expression to extract information
            pattern = r'\[(.*?)\]'
            matches = re.findall(pattern, data_line)

            # Convert data to dictionary
            parsed_data = {key_value.split('=')[0]: key_value.split('=')[1] for key_value in matches}

            return parsed_data

        # Parse each line of data sequentially
        with open(data_path, 'r') as f:

            current_seq = []  # For temporarily storing current sequence
            current_metadata = None  # For temporarily storing current metadata

            for line in f:
                if line.startswith('>'):

                    if current_metadata is not None:
                        current_metadata['sequence'] = ''.join(current_seq)
                        final_result_list.append(current_metadata)
                        current_seq = []  # Reset sequence cache

                    print(line)

                    # Parse NCBI_RNA_id
                    NCBI_RNA_id = merge_NCBI_RNA_id(data_line=line)
                    current_metadata = {'NCBI_RNA_id': NCBI_RNA_id}

                    # Parse other columns
                    else_data = merge_other_tag(data_line=line)
                    current_metadata.update(else_data)
                else:
                    # If not metadata line, consider as sequence line, add to current sequence cache
                    if line and not line.startswith('>'):
                        current_seq.append(line)

        df = pd.DataFrame(final_result_list)
        return df

    # Associate with ref table's NCBI_RefSeq_assembly to get specified columns
    @staticmethod
    def _join_with_ref(df, df_ref):
        rename_columns_dict = {'species': 'species_name'}
        df_ref = df_ref.rename(columns=rename_columns_dict)
        target_columns = [
            'NCBI_RefSeq_assembly',
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
            'lishan_serotype_tax_id',
            'current_scientific_name'
        ]
        df_ref_join = df_ref[target_columns]

        df_final = pd.merge(
            left=df,
            right=df_ref_join,
            on='NCBI_RefSeq_assembly',
            how='left'
        )
        return df_final

    # Associate with csn file management to get lishan_csn_id
    @staticmethod
    def _join_with_csn(df, df_csn):
        target_columns = [
            'lishan_csn_id',
            'current_scientific_name'
        ]
        df_csn_join = df_csn[target_columns]

        df_final = pd.merge(
            left=df,
            right=df_csn_join,
            on='current_scientific_name',
            how='left'
        )
        return df_final

    # 1. Extract all data
    def tar_all_file(self, data_dir, save_dir):
        # Record for non-existing specified files
        path_record_not_exist = save_dir + 'dont_contains_target_rna.txt'
        with open(path_record_not_exist, 'w+', encoding='utf-8') as f_record:
            f_record.write('Following GCF do not have specified RNA file：\n')

        list_GCF_number = os.listdir(data_dir)  # Get GCF collection GCF_000006765.1
        for GCF_number in list_GCF_number:
            detail_dir = data_dir + GCF_number + '/'
            GCF_version = self._select_right_dir_version(detail_dir)

            if GCF_version is None:
                continue

            # Determine if this version data is needed data
            path_conclude = data_dir + f'/{GCF_number}/{GCF_version}/assembly_status.txt'
            is_need = self._conclude_is_need_version(data_path=path_conclude)
            if not is_need:
                continue

            print('########')
            print(GCF_version)

            data_path = data_dir + f'/{GCF_number}/{GCF_version}/{GCF_version}_rna_from_genomic.fna.gz'
            # Determine if file exists
            if not os.path.exists(data_path):
                print('Target RNA file does not exist')
                with open(path_record_not_exist, 'a+', encoding='utf-8') as f_record:
                    f_record.write(GCF_version + '\n')
                continue

            output_file = data_path.split('/')[-1]  # Extraction result file name

            # Storage directory for extraction results
            final_save_dir = save_dir + f'Data/{GCF_number}/{GCF_version}/'
            until.create_dir(final_save_dir)

            # Define file name to extract
            input_file_path = data_path

            # Extract tar.gz file
            if data_path.endswith('tar.gz'):
                with tarfile.open(input_file_path, 'r:gz') as tar:
                    tar.extractall(path=final_save_dir)  # Extract all files to current directory
            # Extract .gz file
            else:
                output_file = output_file.replace('.gz', '')
                save_path = final_save_dir + output_file
                with gzip.open(input_file_path, 'rb') as f_in:
                    with open(save_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

            print(f'{data_path} extraction completed.')

    # 2. Parse all data
    def parse_all_file(self, save_dir_all, data_dir, df_ref, df_csn):
        # Copy record of non-existing target files found during extraction
        scan_path = data_dir + 'dont_contains_target_rna.txt'
        path_save_scan = save_dir_all + 'dont_contains_target_rna.txt'
        shutil.copy(scan_path, path_save_scan)

        # Store all parsing results
        # lishan_csn_id
        # NCBI_RefSeq_assembly
        # NCBI_RefSeq_assembly_version
        mapping_table_result = []
        mapping_file = f'NCBI_RefSeq_assembly_version_csn_mapping.tsv'
        save_path_mapping = save_dir_all + mapping_file

        # Build storage
        final_save_dir = save_dir_all + 'NCBI_RNA_locus_tag/'
        until.create_dir(final_save_dir)

        save_dir_fna = save_dir_all + 'NCBI_RNA_seq/'
        until.create_dir(save_dir_fna)

        # Processing situation
        deal_forsee_locus = final_save_dir + 'Status of data processing.txt'
        with open(deal_forsee_locus, 'w+'):
            pass

        # Parse
        data_dir = data_dir + 'Data/'
        list_GCF_number = os.listdir(data_dir)
        for GCF_number in list_GCF_number:
            detail_dir = data_dir + GCF_number + '/'
            list_GCF_version = os.listdir(detail_dir)
            for GCF_version in list_GCF_version:
                data_path = data_dir + f'{GCF_number}/{GCF_version}/{GCF_version}_rna_from_genomic.fna'
                df = self._parse_process(data_path)

                # Add CGF number and version number
                df['NCBI_RefSeq_assembly'] = GCF_number
                df['NCBI_RefSeq_assembly_version'] = GCF_version

                # Associate with ref table's NCBI_RefSeq_assembly to get specified columns
                df = self._join_with_ref(df, df_ref)

                # Associate with csn table to get lishan_csn_id
                df = self._join_with_csn(df, df_csn)

                # Rename species column
                rename_dict = {
                    'species': 'species_name'
                }
                df = df.rename(columns=rename_dict)

                # Get "lishan_species_tax_id" "lishan_strains_tax_id"
                lishan_species_tax_id = df['lishan_species_tax_id'].tolist()[0]
                lishan_strains_tax_id = df['lishan_strains_tax_id'].tolist()[0]
                lishan_csn_id = df['lishan_csn_id'].tolist()[0]
                d = {
                    'lishan_csn_id': lishan_csn_id,
                    'NCBI_RefSeq_assembly': GCF_number,
                    'NCBI_RefSeq_assembly_version': GCF_version
                }
                mapping_table_result.append(d)

                # Fill empty values
                df = df.fillna('NA_NO')

                # Column headers all lowercase
                df = common_modify.change_columns_to_same_format(df, [])

                # Construct storage path

                save_name = f'{lishan_species_tax_id}_{lishan_strains_tax_id}_NCBI_RNA_locus_tag.tsv'
                save_path = final_save_dir + save_name
                until.save_df(save_path, df)

                save_name_fna = f'{lishan_species_tax_id}_{lishan_strains_tax_id}_NCBI_RNA_seq.fna'
                save_path_fna = save_dir_fna + save_name_fna
                shutil.copy(data_path, save_path_fna)

        df_mapping = pd.DataFrame(mapping_table_result)
        df_mapping = df_mapping.drop_duplicates()
        until.save_df(save_path_mapping, df_mapping)

    def __init__(self, input_dir, ref_path, path_csn, output_dir):
        self.input_dir = input_dir
        self.ref_path = ref_path
        self.path_csn = path_csn
        self.output_dir = output_dir

    def run(self):
        # 1. Extract all data
        # Original data path
        original_data_dir = self.input_dir
        # Storage path for extraction results
        save_dir_tar_res = self.output_dir + f'Stage0_NCBI_RNA_fna_tar_result/'
        until.create_dir(save_dir_tar_res)
        self.tar_all_file(
            data_dir=original_data_dir,
            save_dir=save_dir_tar_res
        )

        # 2. Parse all data
        # Reference file path for supplementing fields
        path_ref = self.ref_path
        df_ref = until.load_df(path_ref)

        # Data for supplementing csn_id
        path_csn = self.path_csn
        df_csn = until.load_df(path_csn)

        # Main storage directory for parsing results
        save_dir_all = f'{self.output_dir}Stage1_NCBI_RNA_fna_extract_result/'
        until.create_dir(save_dir_all)
        self.parse_all_file(
            save_dir_all=save_dir_all,
            data_dir=save_dir_tar_res,
            df_ref=df_ref,
            df_csn=df_csn
        )


if __name__ == '__main__':
    Cleaner = FNAExtractor()
    Cleaner.run()
