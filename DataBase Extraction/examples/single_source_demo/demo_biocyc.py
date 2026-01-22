"""
Demo script for running the biocyc single-source processing pipelines.

Usage example:
python demo_biocyc.py \
    --raw_input_dir ../../sample_data/01_biocyc/raw_data \
    --output_dir= ../../result/single_source_demo/DB_biocyc \
    --ref_path= ../../sample_data/00_reference/reference.tsv \
"""
import argparse
from pipelines.run_single_biocyc import run_biocyc_pipeline


def main():
    parser = argparse.ArgumentParser(description="Demo for biocyc single-source pipelines")

    parser.add_argument(
        "--raw_input_dir",
        required=True,
        help="Directory for raw data input"
    )

    parser.add_argument(
        "--output_dir",
        required=True,
        help="Directory for saving processed results"
    )

    parser.add_argument(
        "--ref_path",
        required=True,
        help="Reference for supplementary microbiome information"
    )

    # args = parser.parse_args()
    # run_biocyc_pipeline(
    #     raw_input_dir=args.raw_input_dir,
    #     output_dir=args.output_dir,
    #     ref_path=args.ref_path,
    # )

    run_biocyc_pipeline(
        raw_input_dir='../../sample_data/02_biocyc/raw_data',
        output_dir='../../result/single_source/DB_biocyc',
        ref_path='../../sample_data/00_reference/reference.tsv',
    )


if __name__ == "__main__":
    main()
