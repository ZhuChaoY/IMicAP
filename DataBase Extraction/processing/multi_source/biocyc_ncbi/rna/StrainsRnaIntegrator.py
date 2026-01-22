import json
import os

import utils.my_df_function as utils
import processing.multi_source.biocyc_ncbi.rna.stage1_prepare_source.prepare_ncbi_info as prepare_ncbi_info
import processing.multi_source.biocyc_ncbi.rna.stage1_prepare_source.prepare_biocyc as prepare_biocyc
from .stage2_columns_processing import columns_processing
from .stage3_synonym_normalization import synonym_normalization
from .stage4_database_link_merge import database_link_merge
from .stage5_ncbi_biocyc_integration import ncbi_biocyc_integration
from .stage6_ncbi_result_consolidation import ncbi_result_consolidation
from .stage7_attribute_enrichment import attribute_enrichment
from .stage8_identifier_generation import identifier_generation
from .stage9_final_standardization import final_standardization


class RnaStrainIntegrator:
    @staticmethod
    def ini_final_standardize(
            file_name_id,
            submit_dir,
            save_dir,
            data_path
    ):
        df_merge = utils.load_df(data_path)

        # Construct storage path
        utils.create_dir(save_dir)

        # Perform 8th processing
        df_merge = final_standardization.standardize_process(df_merge)

        # Store data
        save_name = file_name_id + '_RNA_entity_bio_ncbi.tsv'

        save_path = save_dir + save_name
        utils.save_df(save_path, df_merge)

        save_path_submit = submit_dir + save_name
        utils.save_df(save_path_submit, df_merge)

    def __init__(self,
                 batch_num, sub_id_count, path_ref, path_csn,
                 biocyc_gene_data_dir,
                 biocyc_gene_dir_name,
                 biocyc_rna_data_dir,
                 biocyc_rna_dir_name,
                 ncbi_gene_info_path,
                 output_dir
                 ):
        self.batch_num = batch_num
        self.sub_id_count = sub_id_count

        self.path_ref = path_ref
        self.path_csn = path_csn

        self.test_file_list = ['lishan_817_lishan_272559']

        self.biocyc_gene_data_dir = biocyc_gene_data_dir
        self.biocyc_gene_dir_name = biocyc_gene_dir_name

        self.biocyc_rna_data_dir = biocyc_rna_data_dir
        self.biocyc_rna_dir_name = biocyc_rna_dir_name

        self.ncbi_gene_info_path = ncbi_gene_info_path

        self.output_dir = output_dir

    def run(self):

        # Storage path for Ref
        path_reference = self.path_ref
        df_reference = utils.load_df(path_reference)

        # Storage path for csn table
        path_csn = self.path_csn

        # File for testing
        test_file = self.test_file_list


        # Storage path for NCBI preliminary filtering results
        ncbi_data_path = self.ncbi_gene_info_path

        # Initialize main directory for all data of this processing result
        save_dir_all = self.output_dir
        utils.create_dir(save_dir_all)

        # Storage directory for various auxiliary information
        check_dir = save_dir_all + 'Check/'
        utils.create_dir(check_dir)

        # Storage path for error logs
        error_log_path = check_dir + f'error_log_RNA_strains.txt'
        with open(error_log_path, 'w+'):
            pass

        # Need to store all 8th processing results in same folder
        all_mapping_result_dir = save_dir_all + f'/Mapping/'
        utils.create_dir(all_mapping_result_dir)
        all_mapping_file_summary = all_mapping_result_dir + 'Status of data processing.txt'
        with open(all_mapping_file_summary, 'w+'):
            pass

        # Initialize processing record
        record_path = check_dir + f'/RNA_SOP_result_RNA_strains.json'
        if os.path.exists(record_path):
            with open(record_path, 'r') as f:
                record_dict = json.load(f)
        else:
            record_dict = {}

        def start_run(row):

            # Initialize ID for distinguishing during file storage
            li_shan_species_id = row['lishan_species_tax_id']
            li_shan_strains_id = row['lishan_strains_tax_id']
            file_name_id = li_shan_species_id + '_' + li_shan_strains_id



            # BioCyc data storage path for current bacteria
            biocyc_gene_data_dir = self.biocyc_gene_data_dir + f'{file_name_id}/Gene/{self.biocyc_gene_dir_name}/'
            biocyc_rna_data_dir = self.biocyc_rna_data_dir + f'{file_name_id}/{self.biocyc_rna_dir_name}/'

            # Initialize columns for filling data
            current_scientific_name = row['current_scientific_name']

            sub_strains_tax_id = row['substrains_tax_id']
            strains_tax_id = row['strains_tax_id']
            sero_type_tax_id = row['serotype_tax_id']
            subspecies_tax_id = row['subspecies_tax_id']
            species_tax_id = row['species_tax_id']

            # Initialize columns for filtering NCBI data
            ncbi_target_tax_id_dict = {
                'substrains_tax_id': sub_strains_tax_id,
                'strains_tax_id': strains_tax_id,
                'serotype_tax_id': sero_type_tax_id,
                'subspecies_tax_id': subspecies_tax_id,
                'species_tax_id': species_tax_id
            }

            # Previous processing status
            # if file_name_id in record_dict:
            #     deal_status = record_dict.get(file_name_id)
            #     if deal_status == 'success':
            #         return

            # For use during testing
            # if file_name_id not in test_file:
            #     return
            print(2*'\n')
            print(file_name_id)

            # Initialize storage folder for all processing results of current bacteria
            organism_save_dir = save_dir_all + f'Data/{file_name_id}/'
            utils.create_dir(organism_save_dir)

            # Initialize processing status record
            record_d = {file_name_id: 'success'}

            # Initialize error information
            errors = []
            try:
                print('### Stage1_prepare_source ###')

                save_dir = organism_save_dir + 'stage1_prepare_source/'
                is_exist = prepare_biocyc.deal_gene_of_biocyc(
                    file_name_id=file_name_id,
                    biocyc_gene_data_dir=biocyc_gene_data_dir,
                    biocyc_rna_data_dir=biocyc_rna_data_dir,
                    save_dir=save_dir
                )
                if not is_exist:
                    print('lack biocyc data, pass')
                    return

                save_dir = organism_save_dir + 'stage1_prepare_source/'
                is_exist, df_ncbi = prepare_ncbi_info.prepare_ncbi_data(
                    li_shan_id=file_name_id,
                    ncbi_target_tax_id_dict=ncbi_target_tax_id_dict,
                    ncbi_data_path=ncbi_data_path,
                    save_dir=save_dir
                )
                if not is_exist:
                    print('lack ncbi data, pass')
                    return

                # biocyc
                print('### stage2_columns_processing ###')
                save_dir = organism_save_dir + f'/stage2_columns_processing/'
                utils.create_dir(save_dir)
                df_biocyc = columns_processing.columns_process(
                    file_name_id=file_name_id,
                    save_dir=save_dir,
                    biocyc_gene_data_dir=biocyc_gene_data_dir,
                    biocyc_rna_data_dir=biocyc_rna_data_dir
                )

                # biocyc
                print('### stage3_synonym_normalization ###')
                save_dir = organism_save_dir + 'stage3_synonym_normalization/'
                df_biocyc = synonym_normalization.synonym_normalize(
                    df=df_biocyc,
                    file_name_id=file_name_id,
                    save_dir=save_dir,
                )

                # biocyc
                print('### stage4_database_link_merge ###')
                save_dir = organism_save_dir + 'stage4_database_link_merge/'
                df_biocyc = database_link_merge.merge_process(
                    df=df_biocyc,
                    file_name_id=file_name_id,
                    save_dir=save_dir
                )

                # biocyc ncbi
                print('### stage5_ncbi_biocyc_integration ###')
                save_dir = organism_save_dir + 'stage5_ncbi_biocyc_integration/'
                df_merge, df_ncbi_fail, df_biocyc_fail = ncbi_biocyc_integration.integrate_process(
                    file_name_id=file_name_id,
                    save_dir=save_dir,
                    df_biocyc=df_biocyc,
                    df_ncbi=df_ncbi,
                )

                print('### stage6_ncbi_result_consolidation ###')
                save_dir = organism_save_dir + 'stage6_ncbi_result_consolidation/'
                df_merge = ncbi_result_consolidation.consolidate_process(
                    file_name_id=file_name_id,
                    save_dir=save_dir,
                    df_ncbi_success=df_merge,
                    df_ncbi_fail=df_ncbi_fail
                )

                print('### stage7_attribute_enrichment ###')
                save_dir = organism_save_dir + 'stage7_attribute_enrichment/'
                df_merge, save_path_enrich = attribute_enrichment.enrich_process(
                    file_name_id=file_name_id,
                    current_scientific_name=current_scientific_name,
                    save_dir=save_dir,
                    df_merge=df_merge,
                    df_biocyc_fail=df_biocyc_fail
                )

                print('### stage8_identifier_generation ###')
                # Initialize data for supplementation
                save_dir = organism_save_dir + 'stage8_identifier_generation/'
                self.sub_id_count, save_path_identifier_generation = identifier_generation.generate_process(
                    batch_num=self.batch_num,
                    sub_id_index=self.sub_id_count,
                    file_name_id=file_name_id,
                    df_csn=utils.load_df(path_csn),
                    save_dir=save_dir,
                    ref_row=row,
                    data_path=save_path_enrich
                )

                print('### stage9_final_standardization ###')
                save_dir = organism_save_dir + 'stage9_final_standardization/'
                self.ini_final_standardize(
                    file_name_id=file_name_id,
                    submit_dir=all_mapping_result_dir,
                    save_dir=save_dir,
                    data_path=save_path_identifier_generation
                )
            except Exception as e:
                error_message = f"{str(file_name_id)}, Error occurred: {str(e)}"
                print(error_message)
                errors.append(error_message)
                record_d = {file_name_id: 'Fail'}

            record_dict.update(record_d)
            with open(record_path, 'w+') as f_record:
                json.dump(record_dict, f_record, indent=4)

            # Store error information
            result_status = record_d.get(file_name_id)
            if result_status == 'success':
                print("All functions executed successfully.")
            else:
                if len(errors) > 0:
                    with open(error_log_path, "a+") as error_file:
                        for error in errors:
                            error_file.write(error + "\n")
                    print("Errors have been logged in error_log.txt")
            print('##############')

        df_reference.apply(start_run, axis=1)


if __name__ == '__main__':
    SOP = RnaStrainIntegrator()
    SOP.run()
