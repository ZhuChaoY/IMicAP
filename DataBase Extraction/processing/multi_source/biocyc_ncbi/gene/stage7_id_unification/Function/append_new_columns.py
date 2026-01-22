"""
Editor: INK
Create Time: 2024/6/4 13:07
File Name: append_new_columns.py
Function: 
"""
import utils.my_df_function as until


# Add new column "current_scientific_name", value as corresponding value in Reference for this bacteria
# Add new column species_tax_id, value as corresponding value in Reference for this bacteria
def append_process(df, current_scientific_name, species_tax_id):
    df['current_scientific_name'] = current_scientific_name
    df['species_tax_id'] = species_tax_id
    return df
