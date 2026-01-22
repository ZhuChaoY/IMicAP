"""
Demo script for running the gene-biocyc-ncbi multi-source processing pipelines.

Usage example:
python demo_gene_biocyc_ncbi.py \
    --ref_path= ../../sample_data/00_reference/reference.tsv \
    --csn_path= ../../sample_data/00_reference/csn.tsv \
    --ncbi_info_path= ../../result/single_source_demo/DB_ncbi/ncbi_gene_info/step2_value_normalize/Data/ncbi_info_normalize_result.tsv \
    --biocyc_dir ../../result/single_source_demo/DB_biocyc/step2_gene_normalization/Data/ \
    --output_dir= ../../result/multi_source_demo/biocyc_ncbi/gene/
"""
import argparse
from pipelines.run_multi_gene_biocyc_ncbi import run_gene_biocyc_ncbi_pipeline


def main():
    parser = argparse.ArgumentParser(description="Demo for gene-biocyc-ncbi multi-source pipelines")

    parser.add_argument(
        "--ref_path",
        required=True,
        help="Reference for supplementary microbiome information"
    )

    parser.add_argument(
        "--csn_path",
        required=True,
        help="CSN for supplementary microbiome information"
    )

    parser.add_argument(
        "--ncbi_info_path",
        required=True,
        help="Directory for raw data input"
    )

    parser.add_argument(
        "--biocyc_dir",
        required=True,
        help="Directory containing biocyc gene result files"
    )

    parser.add_argument(
        "--output_dir",
        required=True,
        help="Directory for saving processed results"
    )

    # args = parser.parse_args()
    # run_gene_biocyc_ncbi_pipeline(
    #     ref_path=args.ref_path,
    #     csn_path=args.csn_path,
    #     ncbi_info_path=args.ncbi_info_path,
    #     biocyc_dir=args.biocyc_dir,
    #     output_dir=args.output_dir
    # )

    run_gene_biocyc_ncbi_pipeline(
        ref_path='../../sample_data/00_reference/reference.tsv',
        csn_path='../../sample_data/00_reference/csn.tsv',
        ncbi_info_path='../../result/single_source/DB_ncbi/ncbi_gene_info/step2_value_normalize/Data/ncbi_info_normalize_result.tsv',
        biocyc_dir='../../result/single_source/DB_biocyc/step2_gene_normalization/Data/',
        output_dir='../../result/multi_source/biocyc_ncbi/gene/'
    )


if __name__ == "__main__":
    main()
