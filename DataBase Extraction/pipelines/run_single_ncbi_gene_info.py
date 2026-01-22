import argparse
import os

from processing.single_source.ncbi.gene_info.taxonomy_gene_filter import TaxonGeneFilter
from processing.single_source.ncbi.gene_info.value_normalizer import ValueNormalizer


def run_ncbi_gene_info_pipeline(
        raw_input_path,
        output_dir,
        path_csn,
        chunk_size=20000
):
    """
    File-based pipelines for ncbi single-source processing.
    Each step reads files produced by the previous step.
    """

    step1_out = os.path.join(output_dir, "step1_taxon_gene_filter")
    step2_out = os.path.join(output_dir, "step2_value_normalize")

    # step1_taxon_gene_filter
    step1 = TaxonGeneFilter(
        path_csn=path_csn,
        input_path=raw_input_path,
        out_dir=step1_out,
        chunk_size=chunk_size
    )
    df_info = step1.run()

    # step2_value_normalize
    step2 = ValueNormalizer(
        output_dir=step2_out,
        df_source=df_info
    )
    step2.run()

    print("[INFO] ncbi gene_info pipelines finished.")
    print(f"[INFO] Final output: {step2_out}")


def main():
    parser = argparse.ArgumentParser(description="Run ncbi single-source pipelines")

    parser.add_argument("--raw_input_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--path_csn", required=True)

    # args = parser.parse_args()
    # run_ncbi_gene_info_pipeline(
    #     raw_input_dir=args.raw_input_dir,
    #     output_dir=args.output_dir,
    #     path_csn=args.path_csn
    # )

    run_ncbi_gene_info_pipeline(
        raw_input_path='../sample_data/05_ncbi/GCF',
        output_dir='../result/single_source/db_ncbi/ncbi_gene_info/',
        path_csn='../sample_data/00_reference/csn.tsv'
    )
#
#
# if __name__ == "__main__":
#     main()
