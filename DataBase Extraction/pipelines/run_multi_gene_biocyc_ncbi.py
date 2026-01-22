from processing.multi_source.biocyc_ncbi.gene import StrainsGeneIntegrator
from processing.multi_source.biocyc_ncbi.gene import SpeciesGeneConsolidator


def run_gene_biocyc_ncbi_pipeline(
        ref_path,
        csn_path,
        ncbi_info_path,
        biocyc_dir,
        output_dir
):

    strains_out = f'{output_dir}strains_integrated_result/'
    species_out = f'{output_dir}species_consolidator_result/'

    StrainsProcessor = StrainsGeneIntegrator.StrainIntegrator(
        ref_path=ref_path,
        csn_path=csn_path,
        ncbi_info_path=ncbi_info_path,
        biocyc_dir=biocyc_dir,
        output_dir=strains_out
    )
    StrainsProcessor.run()

    SpeciesProcessor = SpeciesGeneConsolidator.SpeciesConsolidator(
        gene_strains_dir=f"{strains_out}Data/",
        output_dir=species_out,
        ref_path=ref_path
    )
    SpeciesProcessor.run()