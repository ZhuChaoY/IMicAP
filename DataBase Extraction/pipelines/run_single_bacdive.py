import argparse
import os

from processing.single_source.bacdive import nested_processor
from processing.single_source.bacdive.structure_normalize import BacDiveStructureNormalize
from processing.single_source.bacdive.scan_schema import BacDiveSchemaScanner
from processing.single_source.bacdive.data_standardize import BacDiveStandardize


def run_bacdive_pipeline(
        raw_input_dir,
        struct_dir,
        output_dir,
        ref_path,
        csn_path
):
    """
    File-based pipelines for bacdive single-source processing.
    Each step reads files produced by the previous step.
    """

    step1_out = os.path.join(output_dir, "step1_nested_processor")
    step2_out = os.path.join(output_dir, "step2_structure_normalized")
    step3_out = os.path.join(output_dir, "step3_schema_scanned")
    step4_out = os.path.join(output_dir, "step4_standardized")

    # Step 1: Nested processor
    step1 = nested_processor
    step1.run(
        input_dir=raw_input_dir,
        output_dir=step1_out
    )

    # Step 2: Structure normalization
    step2 = BacDiveStructureNormalize(
        struct_dir=struct_dir,
        flatten_res_dir=os.path.join(step1_out, 'data'),
        output_dir=step2_out
    )
    step2.run()

    # Step 3: Schema scanning
    step3 = BacDiveSchemaScanner(
        flatten_dir=os.path.join(step1_out, 'data'),
        processed_dir=step2_out,
        struct_dir=struct_dir,
        out_dir=step3_out,
    )
    step3.run()

    # Step 4: Data standardization
    step4 = BacDiveStandardize(
        input_dir=step2_out,
        output_dir=step4_out,
        ref_path=ref_path,
        CSN_path=csn_path
    )
    step4.run()

    # # Step 5: ID assignment
    # step5 = BacDiveIDAssigner(
    #     input_dir=step3_out,
    #     output_dir=step4_out
    # )
    # step5.run()

    print("[INFO] bacdive pipelines finished.")
    print(f"[INFO] Final output: {step4_out}")


def main():
    parser = argparse.ArgumentParser(description="Run bacdive single-source pipelines")

    parser.add_argument("--raw_input_dir", required=True)
    parser.add_argument("--struct_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--ref_path", required=True)
    parser.add_argument("--csn_path", required=True)

    args = parser.parse_args()

    run_bacdive_pipeline(
        raw_input_dir=args.raw_input_dir,
        struct_dir=args.struct_dir,
        output_dir=args.output_dir,
        ref_path=args.ref_path,
        csn_path=args.csn_path
    )


if __name__ == "__main__":
    main()
