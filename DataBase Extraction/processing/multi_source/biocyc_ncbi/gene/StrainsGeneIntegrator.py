
import json
import os

import utils.my_df_function as utils
from .stage1_prepare_source.prepare_source_data import prepare_biocyc_gene
from .stage1_prepare_source.prepare_source_data import prepare_ncbi_gene
from .stage2_initial_key_alignment.initial_key_alignment import merge_by_primary_key
from .stage3_schema_comparison.schema_comparison import align_schemas
from .stage4_synonym_resolution.synonym_resolution import integrate_synonyms
from .stage5_cross_reference_fusion.cross_reference_fusion import fuse_cross_references
from .stage6_attribute_harmonization.attribute_harmonization import harmonize_attributes
from .stage7_id_unification.id_unification import unify_identifiers
from .stage8_metadata_enrichment.metadata_enrichment import enrich_metadata
from .stage9_final_standardization.final_standardization import finalize_output


class StrainIntegrator:
    def __init__(self, ref_path, csn_path, ncbi_info_path, biocyc_dir, output_dir):
        self.ref_path = ref_path
        self.csn_path = csn_path

        self.ncbi_info_path = ncbi_info_path
        self.biocyc_dir = biocyc_dir

        self.output_dir = output_dir

    def run(self):
        # Ref table for reference
        path_reference = self.ref_path
        df_reference = utils.load_df(path_reference)
        path_csn = self.csn_path
        # NCBI table participating in merging and filtering
        path_ncbi = self.ncbi_info_path

        # Storage directory for BioCyc parsing results
        biocyc_dir = self.biocyc_dir
        biocyc_match_res_list = os.listdir(biocyc_dir)

        # Main path for data storage
        self.output_dir = self.output_dir
        record_dir = f'{self.output_dir}/Record/'
        utils.create_dir(record_dir)

        # Store error logs
        error_log_path = f'{record_dir}/error_log.txt'
        with open(error_log_path, 'w+'):
            pass

        # Processing record
        record_path = f'{record_dir}/Gene_sop_ncbi_combine_result.json'
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

            # Initialize data for data addition
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
            # Only process microbial groups with existing Biocyc data
            if file_name_id not in biocyc_match_res_list:
                return

            errors = []  # Initialize error log
            d = {file_name_id: 'success'}  # Initialize data processing status

            # Initialize storage directory
            organism_dir = os.path.join(self.output_dir, 'Data', file_name_id)
            utils.create_dir(organism_dir)

            # Attempt each processing sequentially
            try:
                save_dir = organism_dir + '/stage1_prepare_source/Biocyc_gene/'
                prepare_biocyc_gene(li_shan_id=file_name_id,
                                    biocyc_dir=biocyc_dir,
                                    save_dir=save_dir)

                save_dir = organism_dir + '/stage1_prepare_source/NCBI_info/'
                prepare_ncbi_gene(
                    li_shan_id=file_name_id,
                    ncbi_data_path=path_ncbi,
                    ncbi_target_tax_id_dict=ncbi_target_tax_id_dict,
                    save_dir=save_dir,
                )

                df_merged = merge_by_primary_key(
                    file_name_id=file_name_id,
                    file_name_save_dir=organism_dir
                )

                save_dir = organism_dir + '/stage3_schema_comparison/'
                df_merged = align_schemas(
                    file_name_id=file_name_id,
                    save_dir=save_dir,
                    df=df_merged
                )

                save_dir = organism_dir + '/stage4_synonym_resolution/'
                df_merged = integrate_synonyms(
                    file_name_id=file_name_id,
                    save_dir=save_dir,
                    df=df_merged
                )

                save_dir = organism_dir + '/stage5_cross_reference_fusion/'
                df_merged = fuse_cross_references(
                    file_name_id=file_name_id,
                    save_dir=save_dir,
                    df=df_merged
                )

                save_dir = organism_dir + '/stage6_attribute_harmonization/'
                harmonize_attributes(
                    file_name_id=file_name_id,
                    save_dir=save_dir,
                    df=df_merged
                )

                save_dir = organism_dir + '/stage7_id_unification/Data/'
                df_merged = unify_identifiers(
                    file_name_id=file_name_id,
                    current_scientific_name=current_scientific_name,
                    species_tax_id=species_tax_id,
                    save_dir=save_dir,
                    df=df_merged
                )

                save_dir = organism_dir + '/stage8_metadata_enrichment/Data/'
                df_merged = enrich_metadata(
                    file_name_id=file_name_id,
                    df_csn=utils.load_df(path_csn),
                    save_dir=save_dir,
                    ref_row=row,
                    df=df_merged
                )

                save_dir = organism_dir + '/stage9_final_standardization/Data/'
                finalize_output(
                    save_dir=save_dir,
                    file_name_id=file_name_id,
                    df=df_merged
                )

            except Exception as e:
                error_message = f"{str(file_name_id)}, Error occurred: {str(e)}"
                errors.append(error_message)
                d = {file_name_id: 'Fail'}

            record_dict.update(d)
            with open(record_path, 'w+') as f:
                json.dump(record_dict, f)

            # Store error information
            result_status = d.get(file_name_id)
            if result_status == 'success':
                print("All functions executed successfully.")
            else:
                if len(errors) > 0:
                    with open(error_log_path, "a+") as f_error:
                        for error in errors:
                            f_error.write(error + "\n")
                    print("Errors have been logged in error_log.txt")

        df_reference.apply(start_run, axis=1)
