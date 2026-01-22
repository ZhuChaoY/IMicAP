"""
Demo script for running the ncbi gene_info single-source processing pipelines.

Usage example:
python demo_ncbi_gene_info.py \
    --path_csn='../../sample_data/00_reference/csn.tsv', \
    --raw_input_path='../../sample_data/05_ncbi/GENE_INFO/', \
    --output_dir='../../result/single_source/DB_ncbi/ncbi_gene_info',
"""
import argparse
from pipelines.run_single_ncbi_gene_info import run_ncbi_gene_info_pipeline


def main():
    parser = argparse.ArgumentParser(description="Demo for ncbi-gene-info single-source pipelines")

    parser.add_argument(
        "--path_csn",
        required=True,
        help="path for reference data input"
    )

    parser.add_argument(
        "--raw_input_path",
        required=True,
        help="path for raw data input"
    )

    parser.add_argument(
        "--output_dir",
        required=True,
        help="Directory for saving processed results"
    )

    args = parser.parse_args()
    run_ncbi_gene_info_pipeline(
        path_csn=args.path_csn,
        raw_input_path=args.raw_input_path,
        output_dir=args.output_dir
    )

    run_ncbi_gene_info_pipeline(
        path_csn='../../sample_data/00_reference/csn.tsv',
        raw_input_path='../../sample_data/05_ncbi/GENE_INFO/All_Data.gene_info.gz',
        output_dir='../../result/single_source/DB_ncbi/ncbi_gene_info',
    )


if __name__ == "__main__":
    main()
