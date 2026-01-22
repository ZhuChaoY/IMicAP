import argparse
import os

from processing.single_source.go import parse_text
from processing.single_source.go import split_table
from processing.single_source.go import normalize_columns
from processing.single_source.go import combine_synonyms


def run_go_pipeline(
        raw_input_path,
        output_dir
):
    """
    File-based pipelines for go single-source processing.
    Each step reads files produced by the previous step.
    """

    step1_out = os.path.join(output_dir, "step1_parse_text")
    step2_out = os.path.join(output_dir, "step2_split_table")
    step3_out = os.path.join(output_dir, "step3_normalize_columns")
    step4_out = os.path.join(output_dir, "step4_combine_synonyms")

    # Step 1: parse_text
    step1 = parse_text
    df = step1.run(
        input_path=raw_input_path,
        out_dir=step1_out
    )

    # Step 2: split_table
    step2 = split_table
    df_info, df_relation = step2.run(df, step2_out)

    # Step 3: normalize_columns
    step3 = normalize_columns
    df_info, df_relation = step3.run(
        step3_out,
        df_info,
        df_relation
    )

    # Step 4: combine_synonyms
    step4 = combine_synonyms
    step4.run(
        step4_out,
        df_info,
        df_relation
    )

    print("[INFO] go pipelines finished.")
    print(f"[INFO] Final output: {step4_out}")


def main():
    parser = argparse.ArgumentParser(description="Run go single-source pipelines")

    parser.add_argument("--raw_input_dir", required=True)
    parser.add_argument("--struct_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--ref_path", required=True)
    parser.add_argument("--csn_path", required=True)

    # args = parser.parse_args()
    # run_go_pipeline(
    #     raw_input_path=args.raw_input_path,
    #     output_dir=args.output_dir,
    # )

    run_go_pipeline(
        raw_input_path='../sample_data/04_go/go_raw.obo',
        output_dir='../result/single_source/DB_go',
    )


if __name__ == "__main__":
    main()
