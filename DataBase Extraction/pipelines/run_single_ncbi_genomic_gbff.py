import argparse
import os

from processing.single_source.ncbi.genomic_gbff.data_extraction import GbffExtractor
from processing.single_source.ncbi.genomic_gbff.data_processing import GbffProcessor
from processing.single_source.ncbi.genomic_gbff.quality_assessment import GbffQualityAssessment


def run_ncbi_gbff_pipeline(
        raw_input_dir,
        output_dir
):
    """
    File-based pipelines for ncbi single-source processing.
    Each step reads files produced by the previous step.
    """

    step1_out = os.path.join(output_dir, "step1_gbff_extraction")
    step2_out = os.path.join(output_dir, "step2_gbff_processing")
    step3_out = os.path.join(output_dir, "step3_gbff_quality_assessment")

    # step1_gbff_extraction
    step1 = GbffExtractor(
        input_dir=raw_input_dir,
        output_dir=step1_out
    )
    step1.run()

    # step2_gbff_processing
    step2 = GbffProcessor(
        input_dir=os.path.join(step1_out, 'Data'),
        output_dir=step2_out
    )
    step2.run()

    # step3_gbff_quality_assessment
    step3 = GbffQualityAssessment(
        input_dir=os.path.join(step2_out, 'Data'),
        output_dir=step3_out
    )
    step3.run()

    print("[INFO] ncbi genomic_gbff pipelines finished.")
    print(f"[INFO] Final output: {step2_out}")


def main():
    parser = argparse.ArgumentParser(description="Run ncbi single-source pipelines")

    parser.add_argument("--raw_input_dir", required=True)
    parser.add_argument("--output_dir", required=True)

    args = parser.parse_args()
    run_ncbi_gbff_pipeline(
        raw_input_dir=args.raw_input_dir,
        output_dir=args.output_dir,
    )

    run_ncbi_gbff_pipeline(
        raw_input_dir='../../sample_data/05_ncbi/GCF',
        output_dir='../../result/single_source_demo/DB_ncbi/ncbi_gbff',
    )


if __name__ == "__main__":
    main()
