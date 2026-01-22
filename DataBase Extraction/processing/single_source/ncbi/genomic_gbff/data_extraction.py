from Bio import SeqIO
import os
import shutil
import pandas as pd
import utils.my_df_function as utils
import tarfile
import gzip


class GbffExtractor:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.tar_result_dir = os.path.join(self.output_dir, 'Check', 'Tar_result')

    # Used to determine if needed file
    # Filter correct data version folders
    @staticmethod
    def select_right_dir_version(gcf_dir):
        gcf_version_list = os.listdir(gcf_dir)

        # When only one data version, directly use without verification
        if len(gcf_version_list) == 1:
            return gcf_version_list[0]

        # If multiple data versions, verify status files within
        for gcf_version in gcf_version_list:
            version_dir = os.path.join(gcf_dir, gcf_version)
            path_version = os.path.join(version_dir, 'assembly_status.txt')
            with open(path_version, 'r') as f:
                text = f.read()
                if 'latest' in text:
                    return gcf_version

        print(gcf_version_list)
        print('Did not find version number meeting requirements!')
        return None

    # Handle dict nesting situation
    def loop_parse_dict(self, pre_key, pre_value, result_dict):
        # Skip references
        if pre_key == 'references':
            return result_dict

        if isinstance(pre_value, dict):

            for column_next in pre_value:
                value_next = pre_value.get(column_next)
                result_dict = self.loop_parse_dict(
                    pre_key=column_next,
                    pre_value=value_next,
                    result_dict=result_dict
                )
        else:
            if isinstance(pre_value, list):
                pre_value = utils.change_list_to_special_data(pre_value)
            d = {
                pre_key: pre_value
            }
            result_dict.update(d)
        return result_dict

    # For parsing basic info data
    def parse_basic_info(self, record):
        # Initialize parsing result
        data_result = {
            'record_name': record.name,
            'record_description': record.description,
            'record_length': len(record),
            'dbxref': utils.change_list_to_special_data(record.dbxrefs)
        }

        # Handle special dict structure
        annotations_data = record.annotations
        if 'structured_comment' in annotations_data:
            annotations_data['structured_comment'] = dict(annotations_data['structured_comment'])

        # Add annotation data sequentially
        for column in annotations_data:
            # print(column)

            data = annotations_data.get(column)

            data_result = self.loop_parse_dict(
                pre_key=column,
                pre_value=data,
                result_dict=data_result
            )

        df = pd.DataFrame([data_result])
        return df

    # Parse Reference data
    @staticmethod
    def parse_reference(record):
        final_reference_result = []
        for reference in record.annotations.get("references", []):
            # print(reference)
            # Initialize a reference processing result
            reference_dict = {
                'record_name': record.name
            }

            # Get all attributes of Reference object
            all_attributes = dir(reference)
            # print(all_attributes)
            custom_attributes = []
            for attribute in all_attributes:
                if attribute.startswith('__'):
                    continue
                else:
                    custom_attributes.append(attribute)
            # print(custom_attributes)

            # Get data from attributes sequentially
            for column in custom_attributes:
                value = getattr(reference, column, None)
                if isinstance(value, list):
                    value = utils.change_list_to_special_data(value)
                    if pd.isnull(value):
                        value = 'NA_NO'

                if len(value) == 0:
                    value = 'NA_NO'
                d = {column: value}
                reference_dict.update(d)

            final_reference_result.append(reference_dict)

        df_reference = pd.DataFrame(final_reference_result)
        df_reference = df_reference.fillna('NA_NO')
        return df_reference

    # For parsing Feature data
    def parse_feature(self, record):
        # Initialize parsing result
        feature_result_list = []

        # Initialize method name collection
        method_name_list = ['extract', 'translate']

        for feature in record.features:
            feature_dict = {
                'record_name': record.name
            }

            # Get all attributes of Reference object
            all_attributes = dir(feature)
            # print(all_attributes)
            custom_attributes = []
            for attribute in all_attributes:
                if attribute.startswith('_'):
                    continue
                else:
                    custom_attributes.append(attribute)
            # print(custom_attributes)

            # Get data from attributes sequentially
            for column in custom_attributes:

                # Skip method names
                if column in method_name_list:
                    continue

                # Get attribute values
                attr_value = getattr(feature, column, 'NA_NO')
                value = attr_value

                # If list need convert to specified data type
                if isinstance(value, list):
                    value = utils.change_list_to_special_data(value)

                    # If after conversion, empty, meaning data empty list, convert to specified empty value
                    if pd.isnull(value):
                        value = 'NA_NO'
                # If empty value, convert to specified empty value
                elif value is None:
                    value = 'NA_NO'
                # If not empty or dict, but other type data int, float, convert to str
                elif not isinstance(value, dict):
                    value = str(value)
                else:
                    pass
                    # print(value)

                # print(column)
                # print(value)
                # print('###')

                # Handle dict nesting situation
                feature_dict = self.loop_parse_dict(
                    pre_key=column,
                    pre_value=value,
                    result_dict=feature_dict
                )

            feature_result_list.append(feature_dict)

        df_feature = pd.DataFrame(feature_result_list)
        df_feature = df_feature.fillna('NA_NO')
        return df_feature

    # Test sample
    def parse_main_process_test(self):
        path_gbff = 'E:/DWD/ncbi/NCBI_gbff_parse_v1_20241121/Source/GCF_000723465.1_Rb803_genomic.genomic_gbff'
        save_dir = 'E:/DWD/ncbi/NCBI_gbff_parse_test_v2_20241125/Data/'
        utils.create_dir(save_dir)

        # Initialize processing result
        df_basic_result = pd.DataFrame()
        df_reference_result = pd.DataFrame()
        df_feature_result = pd.DataFrame()

        # Parse GenBank file
        for record in SeqIO.parse(path_gbff, "genbank"):
            # print("Sequence name:", record.name)
            # print("Sequence description:", record.description)
            # print("Sequence length:", len(record))
            # print("Sequence features-count:", len(record.features))
            # print("Sequence annotations:", record.annotations)
            # for feature in record.features:
            #     print("Feature type:", feature.type)
            #     print("Feature location:", feature.location)
            #     print("Feature qualifiers:", feature.qualifiers)
            #     print('.........')

            # for reference in record.annotations.get("references", []):
            #     journal = None
            #     pubmed = None
            #     # print(type(reference))
            #     # all_attributes = dir(reference)

            df_basic_info = self.parse_basic_info(record)
            df_basic_result = pd.concat([df_basic_info, df_basic_result])
            df_basic_result = df_basic_result.fillna('NA_NO')

            # df_reference = parse_reference(record)
            # df_reference_result = pd.concat([df_reference, df_reference_result])
            # df_reference_result = df_reference_result.fillna('NA_NO')
            #
            # df_feature = parse_feature(record)
            # df_feature_result = pd.concat([df_feature, df_feature_result])
            # df_feature_result = df_feature_result.fillna('NA_NO')

            print('#######')
        # save_path = 'test.tsv'
        # until.save_df(save_path, df_basic_result)

        # save_path = './test_ref.tsv'
        # until.save_df(save_path, df_reference_result)

        # save_path = './test_feature.tsv'
        # until.save_df(save_path, df_feature_result)

    # Extract files
    def tar_file(self):
        # Original data path
        data_dir = self.input_dir

        # Storage path for extraction results
        save_dir = self.tar_result_dir
        utils.create_dir(save_dir)

        # Record for non-existing specified files
        path_record_not_exist = os.path.join(save_dir, 'dont_contains_target_gbff.txt')
        with open(path_record_not_exist, 'w+', encoding='utf-8') as f_record:
            f_record.write('Following GCF do not have specified gbff file：\n')

        list_GCF_number = os.listdir(data_dir)  # Get GCF collection GCF_000006765.1

        for GCF_number in list_GCF_number:
            detail_dir = os.path.join(data_dir, GCF_number)

            # Determine if this version data is needed data
            GCF_version = self.select_right_dir_version(gcf_dir=detail_dir)
            path_conclude = os.path.join(data_dir, GCF_number, GCF_version, 'assembly_status.txt')
            if not GCF_version:
                continue

            print('########')
            print(GCF_version)

            data_path = os.path.join(data_dir, GCF_number, GCF_version, f'{GCF_version}_genomic.gbff.gz')
            print(data_path)
            # Determine if file exists
            if not os.path.exists(data_path):
                print('Target gbff file does not exist')
                with open(path_record_not_exist, 'a+', encoding='utf-8') as f_record:
                    f_record.write(GCF_version + '\n')
                continue

            output_file = os.path.basename(data_path)  # Extraction result file name

            # Storage directory for extraction results
            final_save_dir = os.path.join(save_dir, 'Data', GCF_number, GCF_version)
            utils.create_dir(final_save_dir)

            # Define file name to extract
            input_file_path = data_path

            # Extract tar.gz file
            if data_path.endswith('tar.gz'):
                with tarfile.open(input_file_path, 'r:gz') as tar:
                    tar.extractall(path=final_save_dir)  # Extract all files to current directory
            # Extract .gz file
            else:
                output_file = output_file.replace('.gz', '')
                save_path = os.path.join(final_save_dir, output_file)
                with gzip.open(input_file_path, 'rb') as f_in:
                    with open(save_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

            print(f'{data_path} extraction completed.')

    # Main program
    def run(self):
        save_dir_all = self.output_dir

        # Record data
        record_dir = os.path.join(save_dir_all, 'Record')
        utils.create_dir(record_dir)
        record_dict_list = []

        # Storage path
        save_dir = os.path.join(save_dir_all, 'Data')
        utils.create_dir(save_dir)

        # Extract files
        self.tar_file()

        # Storage path for extraction results
        tar_dir = self.tar_result_dir
        utils.create_dir(tar_dir)

        path_list = utils.get_file_path_list(tar_dir, [])

        for path_gbff in path_list:
            if 'dont_contains_target_gbff.txt' in path_gbff:
                continue

            print('#######')
            print(path_gbff)
            # Initialize processing result
            df_basic_result = pd.DataFrame()
            df_reference_result = pd.DataFrame()
            df_feature_result = pd.DataFrame()

            # Birth storage path
            gcf_dir = os.path.dirname(path_gbff)
            gcf_name = os.path.basename(gcf_dir)
            print(gcf_name)
            save_dir_gcf = os.path.join(save_dir, gcf_name)
            utils.create_dir(save_dir_gcf)

            # Parse GenBank file
            for record in SeqIO.parse(path_gbff, "genbank"):
                df_basic_info = self.parse_basic_info(record)
                df_basic_result = pd.concat([df_basic_info, df_basic_result])
                df_basic_result = df_basic_result.fillna('NA_NO')

                df_reference = self.parse_reference(record)
                df_reference_result = pd.concat([df_reference, df_reference_result])
                df_reference_result = df_reference_result.fillna('NA_NO')

                df_feature = self.parse_feature(record)
                df_feature_result = pd.concat([df_feature, df_feature_result])
                df_feature_result = df_feature_result.fillna('NA_NO')

            save_name_basic = 'ncbi_gbff_basic_data.tsv'
            save_path = os.path.join(save_dir_gcf, save_name_basic)
            utils.save_df(save_path, df_basic_result)

            save_name_ref = 'ncbi_gbff_reference.tsv'
            save_path = os.path.join(save_dir_gcf, save_name_ref)
            utils.save_df(save_path, df_reference_result)

            save_name_feature = 'ncbi_gbff_feature.tsv'
            save_path = os.path.join(save_dir_gcf, save_name_feature)
            utils.save_df(save_path, df_feature_result)

            # Initialize processing record
            record_d = {
                'GCF_version': gcf_name,
                save_name_basic: len(df_basic_result),
                save_name_ref: len(df_reference_result),
                save_name_feature: len(df_feature_result)
            }
            record_dict_list.append(record_d)

        save_name_record = 'record.tsv'
        save_path = os.path.join(record_dir, save_name_record)
        df_record = pd.DataFrame(record_dict_list)
        utils.save_df(save_path, df_record)

