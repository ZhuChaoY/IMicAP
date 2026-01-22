import argparse
import os

import utils.my_df_function
from processing.single_source.microbewiki import flatten_columns
from processing.single_source.microbewiki import unite_columns
from processing.single_source.microbewiki import supply_columns
from processing.single_source.microbewiki import split_table


def run_microbewiki_pipeline(
        raw_input_dir,
        output_dir,
        ref_path,
        csn_path
):
    """
    File-based pipelines for microbewiki single-source processing.
    Each step reads files produced by the previous step.
    """

    step1_out = os.path.join(output_dir, "step1_flatten_columns")
    step2_out = os.path.join(output_dir, "step2_unite_columns")
    step3_out = os.path.join(output_dir, "step3_supply_columns")
    step4_out = os.path.join(output_dir, "step4_split_table")

    started_time = utils.my_df_function.get_now_time()

    # Step 1: flatten columns
    step1_name = 'Microbewiki_columns_flatten.tsv'
    step1 = flatten_columns.MicrobeWikiFlatten(
        input_dir=raw_input_dir,
        output_dir=step1_out,
        save_name=step1_name
    )
    step1.run()

    # Step 2: unite columns
    step2_name = 'Microbewiki_columns_unite.tsv'
    step2 = unite_columns.MicrobeWikiUniteColumns(
        input_path=os.path.join(step1_out, step1_name),
        output_dir=step2_out,
        save_name=step2_name
    )
    step2.run()

    # Step 3: Supply Columns
    step3_name = 'Microbewiki_columns_supply.tsv'
    step3 = supply_columns.MicrobeWikiSupplyColumns(
        ref_path=ref_path,
        csn_path=csn_path,
        input_path=os.path.join(step2_out, step2_name),
        output_dir=step3_out,
        save_name=step3_name
    )
    step3.run()

    # Step 4: Split Table
    step4 = split_table.MicrobeWikiSplitTable(
        input_path=os.path.join(step3_out, step3_name),
        output_dir=step4_out
    )
    step4.run()

    print("[INFO] microbewiki pipelines finished.")
    print(f"[INFO] Final output: {step4_out}")


def main():
    parser = argparse.ArgumentParser(description="Run microbewiki single-source pipelines")

    parser.add_argument("--raw_input_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--ref_path", required=True)
    parser.add_argument("--csn_path", required=True)

    args = parser.parse_args()

    run_microbewiki_pipeline(
        raw_input_dir=args.raw_input_dir,
        output_dir=args.output_dir,
        ref_path=args.ref_path,
        csn_path=args.csn_path
    )


if __name__ == "__main__":
    main()
