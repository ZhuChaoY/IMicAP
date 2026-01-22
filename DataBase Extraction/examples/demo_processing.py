"""
This script integrates all processing demo flows from the multi_source_demo and single_source_demo folders.
Each workflow is organized as an independent function and can be called as needed.
"""
# Multi-source demo imports
from pipelines.run_multi_gene_biocyc_ncbi import run_gene_biocyc_ncbi_pipeline
from pipelines.run_multi_rna_biocyc_ncbi import run_rna_biocyc_ncbi_pipeline
# Single-source demo imports
from pipelines.run_single_bacdive import run_bacdive_pipeline
from pipelines.run_single_biocyc import run_biocyc_pipeline
from pipelines.run_single_go import run_go_pipeline
from pipelines.run_single_microbewiki import run_microbewiki_pipeline
from pipelines.run_single_ncbi_genomic_gbff import run_ncbi_gbff_pipeline
from pipelines.run_single_ncbi_gene_info import run_ncbi_gene_info_pipeline


# Multi-source demo functions
def demo_gene_biocyc_ncbi():
    """Gene Biocyc-NCBI multi-source processing demo"""
    run_gene_biocyc_ncbi_pipeline(
        ref_path='../sample_data/00_reference/reference.tsv',
        csn_path='../sample_data/00_reference/csn.tsv',
        ncbi_info_path='../result/single_source/db_ncbi/ncbi_gene_info/step2_value_normalize/Data/ncbi_info_normalize_result.tsv',
        biocyc_dir='../result/single_source/db_biocyc/step2_gene_normalization/Data/',
        output_dir='../result/multi_source/biocyc_ncbi/gene/'
    )


def demo_rna_biocyc_ncbi():
    """RNA Biocyc-NCBI multi-source processing demo"""
    run_rna_biocyc_ncbi_pipeline(
        batch_num='B3',
        sub_id_count=1,
        path_ref='../sample_data/00_reference/reference.tsv',
        path_csn='../sample_data/00_reference/csn.tsv',
        biocyc_gene_data_dir='../result/single_source/DB_biocyc/step2_gene_normalization/Data/',
        biocyc_gene_dir_name='Biocyc_gene_3rd_result',
        biocyc_rna_data_dir='../result/single_source/DB_biocyc/step1_structure_normalization/Data/',
        biocyc_rna_dir_name='Biocyc_4th_result/Data/gene/',
        ncbi_gene_info_path='../result/single_source/DB_ncbi/ncbi_gene_info/step2_value_normalize/Data/ncbi_info_normalize_result.tsv',
        output_dir='../result/multi_source/biocyc_ncbi/rna/'
    )


# Single-source demo functions
def demo_bacdive():
    """BacDive single-source processing demo"""
    run_bacdive_pipeline(
        raw_input_dir='../sample_data/01_bacdive/raw_data',
        struct_dir='../sample_data/01_bacdive/config/structure',
        ref_path='../sample_data/00_reference/reference.tsv',
        csn_path='../sample_data/00_reference/csn.tsv',
        output_dir='../result/single_source/db_bacdive',

    )


def demo_biocyc():
    """Biocyc single-source processing demo"""
    run_biocyc_pipeline(
        raw_input_dir='../sample_data/02_biocyc/raw_data',
        ref_path='../sample_data/00_reference/reference.tsv',
        output_dir='../result/single_source/db_biocyc',
    )


def demo_go():
    """GO single-source processing demo"""
    run_go_pipeline(
        raw_input_path='../sample_data/04_go/go_raw.obo',
        output_dir='../result/single_source/db_go',
    )


def demo_microbewiki():
    """MicrobeWiki single-source processing demo"""
    run_microbewiki_pipeline(
        raw_input_dir='../sample_data/03_microbewiki/raw_data',
        ref_path='../sample_data/00_reference/reference.tsv',
        csn_path='../sample_data/00_reference/csn.tsv',
        output_dir='../result/single_source/db_microbewiki',
    )


def demo_ncbi_gene_info():
    """NCBI GENE-INFO single-source processing demo"""
    run_ncbi_gene_info_pipeline(
        raw_input_path='../sample_data/05_ncbi/GENE_INFO/All_Data.gene_info.gz',
        path_csn='../sample_data/00_reference/csn.tsv',
        output_dir='../result/single_source/db_ncbi/ncbi_gene_info/',

    )


def demo_ncbi_gbff():
    """NCBI Genomic GBFF single-source processing demo"""
    run_ncbi_gbff_pipeline(
        raw_input_dir='../sample_data/05_ncbi/GCF',
        output_dir='../result/single_source/db_ncbi/ncbi_gbff',
    )


if __name__ == "__main__":
    # Example: call the corresponding workflow as needed

    demo_bacdive()
    demo_biocyc()
    demo_go()
    demo_microbewiki()
    demo_ncbi_gene_info()
    demo_ncbi_gbff()

    demo_gene_biocyc_ncbi()
    demo_rna_biocyc_ncbi()
    pass
