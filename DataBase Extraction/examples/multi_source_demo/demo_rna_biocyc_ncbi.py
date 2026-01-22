"""
Demo script for running the rna-biocyc-ncbi multi-source processing pipelines.

Usage example:
python demo_rna_biocyc_ncbi.py \
    --ref_path= ../../sample_data/00_reference/reference.tsv \
    --csn_path= ../../sample_data/00_reference/csn.tsv \
    --ncbi_info_path= ../../result/single_source_demo/DB_ncbi/ncbi_rna_info/step2_value_normalize/Data/ncbi_info_normalize_result.tsv \
    --biocyc_dir ../../result/single_source_demo/DB_biocyc/step2_rna_normalization/Data/ \
    --output_dir= ../../result/multi_source_demo/biocyc_ncbi/rna/
"""
import argparse
from pipelines.run_multi_rna_biocyc_ncbi import run_rna_biocyc_ncbi_pipeline


def main():
    parser = argparse.ArgumentParser(description="Demo for rna-biocyc-ncbi multi-source pipelines")

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

    parser.add_argument(
        "--biocyc_gene_data_dir",
        required=True,
        help="Directory for biocyc data input"
    )
    parser.add_argument(
        "--biocyc_gene_dir_name",
        required=True,
        help="Directory for biocyc data input"
    )
    parser.add_argument(
        "--biocyc_rna_data_dir",
        required=True,
        help="Directory for biocyc data input"
    )
    parser.add_argument(
        "--biocyc_rna_dir_name",
        required=True,
        help="Directory for biocyc data input"
    )
    parser.add_argument(
        "--ncbi_gene_info_path",
        required=True,
        help="Directory for ncbi data input"
    )

    parser.add_argument(
        "--batch_num",
        required=True,
        help="data batch nums"
    )
    parser.add_argument(
        "--main table id index",
        required=True,
        help="Directory for ncbi data input"
    )

    parser.add_argument(
        "--output_dir",
        required=True,
        help="Directory for saving processed results"
    )

    # args = parser.parse_args()
    # run_rna_biocyc_ncbi_pipeline(
    #     path_ref=args.path_ref,
    #     path_csn=args.path_csn,
    #
    #     biocyc_gene_data_dir=args.biocyc_gene_data_dir,
    #     biocyc_gene_dir_name=args.biocyc_gene_dir_name,
    #     biocyc_rna_data_dir=args.biocyc_rna_data_dir,
    #     biocyc_rna_dir_name=args.biocyc_rna_dir_name,
    #
    #     ncbi_gene_info_path=args.ncbi_gene_info_path,
    #
    #     output_dir=args.output_dir,
    #
    #     batch_num=args.batch_num,
    #     sub_id_count=args.sub_id_count
    # )

    run_rna_biocyc_ncbi_pipeline(
        batch_num='B3',
        sub_id_count=1,
        path_ref='D:/MyCode/Code/CodeForPaper/sample_data/00_reference/reference.tsv',
        path_csn='D:/MyCode/Code/CodeForPaper/sample_data/00_reference/csn.tsv',
        biocyc_gene_data_dir='D:/MyCode/Code/CodeForPaper/result/single_source/DB_biocyc/step2_gene_normalization/Data/',
        biocyc_gene_dir_name='Biocyc_gene_3rd_result',
        biocyc_rna_data_dir='D:/MyCode/Code/CodeForPaper/result/single_source/DB_biocyc/step1_structure_normalization/Data/',
        biocyc_rna_dir_name='Biocyc_4th_result/Data/gene/',
        ncbi_gene_info_path='D:/MyCode/Code/CodeForPaper/result/single_source/DB_ncbi/ncbi_gene_info/step2_value_normalize/Data/ncbi_info_normalize_result.tsv',
        output_dir='D:/MyCode/Code/CodeForPaper/result/multi_source/biocyc_ncbi/rna/'
    )


if __name__ == "__main__":
    main()
