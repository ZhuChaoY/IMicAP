"""
Demo script for running the go single-source processing pipelines.

Usage example:
python demo_go.py \
    --raw_input_dir ../../sample_data/04_go/raw_data \
    --output_dir= ../../result/single_source_demo/
"""
import argparse
from pipelines.run_single_go import run_go_pipeline


def main():
    parser = argparse.ArgumentParser(description="Demo for go single-source pipelines")

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
    # run_go_pipeline(
    #     raw_input_path=args.raw_input_path,
    #     output_dir=args.output_dir,
    #
    # )

    run_go_pipeline(
        raw_input_path='../../sample_data/04_go/go_raw.obo.gz',
        output_dir='../../result/single_source/DB_go',
    )


if __name__ == "__main__":
    main()
