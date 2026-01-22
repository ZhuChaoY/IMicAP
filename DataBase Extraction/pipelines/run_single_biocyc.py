import argparse
import os

import utils.my_df_function
from processing.single_source.biocyc import structure_normalization
from processing.single_source.biocyc import gene_normalization
from processing.single_source.biocyc import protein_normalization


def run_biocyc_pipeline(
        raw_input_dir,
        output_dir,
        ref_path,
):
    """
    File-based pipelines for biocyc single-source processing.
    Each step reads files produced by the previous step.
    """

    step1_out = os.path.join(output_dir, "step1_structure_normalization")
    step2_out = os.path.join(output_dir, "step2_gene_normalization")
    step3_out = os.path.join(output_dir, "step3_protein_normalization")

    started_time = utils.my_df_function.get_now_time()

    # Step 1: structure normalization
    step1 = structure_normalization.StructureNormalization(
        ref_path=ref_path,
        input_dir=raw_input_dir,
        output_dir=step1_out
    )
    step1.run()

    # Step 2: gene normalization
    step2 = gene_normalization.GeneNormalize(
        ref_path=ref_path,
        input_dir=step1_out,
        out_dir=step2_out
    )
    step2.run()

    # Step 3: protein normalization
    step3 = protein_normalization.ProteinNormalize(
        ref_path=ref_path,
        input_dir=step1_out,
        output_dir=step3_out
    )
    step3.run()

    print("[INFO] biocyc pipelines finished.")
    print(f"[INFO] Final output: {step3_out}")


def main():
    parser = argparse.ArgumentParser(description="Run biocyc single-source pipelines")

    parser.add_argument("--raw_input_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--ref_path", required=True)

    args = parser.parse_args()

    run_biocyc_pipeline(
        raw_input_dir=args.raw_input_dir,
        output_dir=args.output_dir,
        ref_path=args.ref_path,
    )


if __name__ == "__main__":
    main()
