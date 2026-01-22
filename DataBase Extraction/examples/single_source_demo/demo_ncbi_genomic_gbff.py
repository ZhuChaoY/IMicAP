"""
Demo script for running the ncbi genomic_gbff single-source processing pipelines.

Usage example:
python demo_ncbi_genomic_gbff.py \
    --raw_input_dir='../sample_data/05_ncbi/GCF' \
    --output_dir='../result/single_source_demo/DB_ncbi/ncbi_gbff'
"""
import argparse
from pipelines.run_single_ncbi_genomic_gbff import run_ncbi_gbff_pipeline


def main():
    parser = argparse.ArgumentParser(description="Demo for ncbi-genomic_gbff single-source pipelines")

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

    # args = parser.parse_args()
    # run_ncbi_gbff_pipeline(
    #     raw_input_dir=args.raw_input_dir,
    #     output_dir=args.output_dir,
    # )

    run_ncbi_gbff_pipeline(
        raw_input_dir='../../sample_data/05_ncbi/GCF',
        output_dir='../../result/single_source/DB_ncbi/ncbi_gbff',
    )


if __name__ == "__main__":
    main()
