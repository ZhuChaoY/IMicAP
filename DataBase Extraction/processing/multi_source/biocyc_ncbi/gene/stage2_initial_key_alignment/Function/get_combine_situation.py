"""
Editor: INK
Create Time: 2024/6/19 15:26
File Name: get_combine_situation.py
Function: Get merge overview situation
"""
import pandas as pd

import utils.my_df_function as until


def get_process(df_biocyc, df_ncbi, save_path, total_matched, df_ncbi_fail, df_biocyc_fail):
    # Count matched and unmatched records
    total_biocyc = len(df_biocyc)
    biocyc_fail_count = len(df_biocyc_fail)
    biocyc_success_count = total_biocyc - biocyc_fail_count

    total_ncbi = len(df_ncbi)
    ncbi_fail_count = len(df_ncbi_fail)
    ncbi_success_count = total_ncbi - ncbi_fail_count

    # Output result
    print(f"BioCyc total {total_biocyc} records, matched {biocyc_success_count}, unmatched {biocyc_fail_count}")
    print(f"NCBI total {total_ncbi} records, matched {ncbi_success_count}, unmatched {ncbi_fail_count}")

    with open(save_path, 'w+') as f:
        f.write(
            'biocyc total ' + str(total_biocyc) + ' records, matched ' + str(biocyc_success_count) + ', unmatched ' + str(
                biocyc_fail_count) + '\n')
        f.write('NCBI total ' + str(total_ncbi) + ' records, matched ' + str(ncbi_success_count) + ', unmatched ' + str(
            ncbi_fail_count) + '\n')
