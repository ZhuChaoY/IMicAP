from processing.multi_source.biocyc_ncbi.rna import StrainsRnaIntegrator
from processing.multi_source.biocyc_ncbi.rna import SpeciesRnaConsolidator


def run_rna_biocyc_ncbi_pipeline(
        biocyc_gene_data_dir,
        biocyc_gene_dir_name,
        biocyc_rna_data_dir,
        biocyc_rna_dir_name,
        ncbi_gene_info_path,
        output_dir,
        path_ref,
        path_csn,
        batch_num,
        sub_id_count
):

    strains_out = f'{output_dir}strains_integrated_result/'
    species_out = f'{output_dir}species_consolidator_result/'

    StrainsProcessor = StrainsRnaIntegrator.RnaStrainIntegrator(
        biocyc_gene_data_dir=biocyc_gene_data_dir,
        biocyc_gene_dir_name=biocyc_gene_dir_name,
        biocyc_rna_data_dir=biocyc_rna_data_dir,
        biocyc_rna_dir_name=biocyc_rna_dir_name,
        ncbi_gene_info_path=ncbi_gene_info_path,
        output_dir=strains_out,
        path_ref=path_ref,
        path_csn=path_csn,
        batch_num=batch_num,
        sub_id_count=sub_id_count
    )
    StrainsProcessor.run()

    SpeciesRnaConsolidator.run(
        output_dir=species_out,
        path_csn=path_csn,
        strains_res_dir=strains_out
    )
