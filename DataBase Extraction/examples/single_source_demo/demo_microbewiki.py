"""
Demo script for running the microbewiki single-source processing pipelines.

Usage example:
python demo_microbewiki.py \
    --raw_input_dir ../../sample_data/01_microbewiki/raw_data \
    --output_dir= ../../result/single_source_demo/DB_microbewiki \
    --ref_path= ../../sample_data/00_reference/reference.tsv \
    --csn_path= ../../sample_data/00_reference/csn.tsv
"""
import argparse
from pipelines.run_single_microbewiki import run_microbewiki_pipeline


def main():
    parser = argparse.ArgumentParser(description="Demo for microbewiki single-source pipelines")

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

    parser.add_argument(
        "--csn_path",
        required=True,
        help="CSN for supplementary microbiome information"
    )

    # args = parser.parse_args()
    # run_microbewiki_pipeline(
    #     raw_input_dir=args.raw_input_dir,
    #     output_dir=args.output_dir,
    #     ref_path=args.ref_path,
    #     csn_path=args.csn_path
    # )

    run_microbewiki_pipeline(
        raw_input_dir='../../sample_data/03_microbewiki/raw_data',
        output_dir='../../result/single_source/DB_microbewiki',
        ref_path='../../sample_data/00_reference/reference.tsv',
        csn_path='../../sample_data/00_reference/csn.tsv'
    )


if __name__ == "__main__":
    main()
