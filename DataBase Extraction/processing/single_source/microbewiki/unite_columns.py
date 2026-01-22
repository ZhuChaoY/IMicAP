import ast
import os.path

import pandas as pd
import utils.my_df_function as utils


class MicrobeWikiUniteColumns:

    # Build list data
    @staticmethod
    def build_list(data):
        if pd.isnull(data):
            return []
        else:
            try:
                new_data = ast.literal_eval(data)
                if type(new_data) == list:
                    return new_data
                else:
                    return [data]
            except:
                return [data]

    # Connect list with newline to form final text
    @staticmethod
    def build_final_str(data):
        if not isinstance(data, list):
            return data

        data_str = '\n'.join(data)
        return data_str

    def __init__(self, input_path, output_dir, save_name):
        self.input_path = input_path
        self.output_dir = output_dir
        utils.create_dir(self.output_dir)
        self.save_name = save_name

    def run(self):
        data_path = self.input_path
        df = utils.load_df(data_path)

        # Delete specified columns
        drop_col = [
            'Classification.Classification',
            'Classification.Bacillus_amyloliquefaciens',
            'Author',
            'encoding',
            'Classification.Higher_order_taxa',
            'Classification.Species',
            'Classification.Higher_order_taxa:',
            'Characteristics.Characteristics',
            'Classification.Higher_Order_Taxa',
            '1._Classification.1._Classification',
            '1._Classification.a._Higher_order_taxa',
            'Classification.Higher_order_taxa.Higher_order_taxa',
            'Classification.Higher_order_taxa.Species',
            'Authors',
            'Classification',
            'Species',
            'Classification.Ralstonia_Picketti',
            'Classification.Major_Strains_of_Saccharomyces_cerevisiae',
            'Classification.Genus_species',
        ]
        contains_col_list = df.columns.tolist()
        temp_col = []
        for column in drop_col:
            if column in contains_col_list:
                temp_col.append(column)
        drop_col = temp_col
        df = df.drop(columns=drop_col)

        # Merge columns
        combine_dict = {
            'description_and_significance': [
                'Description_and_Significance',
                'Description_and_significance',
                'Description_and_significance.Description_and_significance',
                'Description_and_significance.Ecology',
                'Description_and_significance.Pathology',
                'Description_and_significance.Metabolism',
                'Characteristics.General_Background',
                'Characteristics.Relatedness_to_other_species_in_the_Bacteroides_genus',
                'Description',
                'Discovery',
                'Relevance',
                'Habitat',
                '2._Description_and_significance',
                'History',
            ],
            'cell_structure_and_metabolism': [
                'Structure,_Metabolism,_and_Life_Cycle',

                'Genome_structure',

                'Cell_structure_and_metabolism',

                'Metabolism',

                'Characteristics.Morphology',

                'Characteristics.Molecular_structure',

                'Characteristics.Metabolism_and_growth',

                'Cell_Structure,_Metabolism,_and_Life_Cycle',

                'Cell_Structure_and_Metabolism',

                'Nutrition_and_metabolism',

                '4._Cell_structure',

                '5._Metabolic_processes',

                'Cell_structure',

                'Metabolic_processes',

                'Cell_structure_and_Metabolism',

                'Cell_and_colony_structure',

                'Cell_Structure,_Metabolism_and_Life_Cycle',

                'Cell_structure_and_metabolism.Cell_structure_and_metabolism',

                'Cell_structure_and_metabolism.Cell_Wall',

                'Cell_structure_and_metabolism.Metabolism',

                'Cell_Structure_and_Metabolic_Processes',

                'Cell_Structure_and_Arrangement',
            ],
            'ecology_and_pathogenesis': [
                'Ecology_and_Pathogenesis',
                'Ecology',
                'Pathology',
                '6._Ecology',
                '7._Pathology',
                'Ecology_and_Pathology',
                '5._Ecology',
                'Pathogenesis',
                'Ecology_and_Biotechnology',
                'Ecology_and_Application_to_Biochemistry',
            ],
            'references': [
                'References',
                'Genome.References',
                '10._References',
                '9._References',
            ],
            'application_to_biotechnology': [
                'Application_to_Biotechnology',

                '8._Medical_Applications',

                'Bioaugmentation_Applications',

                'Application_to_biotechnology',

                'Applications',

                'Application_to_Biotech',
            ],
            'current_research': [
                'Current_Research',
                'Genome.Current_Research',
                '9._Current_Research',
                'Current_research',
                '8._Current_Research.8._Current_Research',
                '8._Current_Research.a._Acne_treatment',
                '8._Current_Research.b._Probiotic_use',
                '8._Current_Research.c._Fighting_against_foodborne_illness',
                '8._Current_Research.d._Inflammatory_Bowel_Disease_ulcerative_colitis_treatment',
                '8._Current_Research.e._Halitosis_treatment',
                '8._Current_Research',
                'Current_Research.Current_Research',
                'Current_Research.Lantana_camara_used_as_substrate_for_fuel_ethanol_production',
                'Current_Research.Increased_glycolytic_flux_due_to_whole-genome_duplication',
                'Current_Research.Effects_of_Aneuploidy_on_Cellular_Physiology_and_Cell_Division_in_Haploid_Yeast',
                'Current_Studies',
                'Current_Research_and_or_Application_to_Biotechnology',
            ],
            'genome_structure': [
                'Genome.Genome',
                'Genome_Structure',
                'Genome_and_genetics',
                '3._Genome_structure',
                '16S_Ribosomal_RNA_Gene_Information',
            ],
            'interesting_feature': [
                'Interesting_facts'
            ]

        }

        # Fill empty values for subsequent merge
        df = df.fillna('[]')
        for final_col in combine_dict:
            # Initialize final columns
            df[final_col] = '[]'
            # Convert from str to list
            df[final_col] = df[final_col].apply(lambda x: self.build_list(x))

            # Merge all sub-columns
            child_columns = combine_dict.get(final_col)
            for column in child_columns:
                if column in df.columns:
                    df[column] = df[column].apply(lambda x: self.build_list(x))
                    df[final_col] = df[final_col] + df[column]

            # Process multi-line content into line-by-line text
            df[final_col] = df[final_col].apply(lambda x: self.build_final_str(x))

        df['species_name'] = df['source_file'].str.replace('MicrobeWiki_', '')
        df['species_name'] = df['species_name'].str.replace('.json', '')

        final_columns_list = list(combine_dict.keys()) + ['species_name', 'source_file']
        print(final_columns_list)
        df = df[final_columns_list]
        df = df.fillna('NA_NO')

        save_dir = self.output_dir
        utils.create_dir(save_dir)
        save_path = os.path.join(save_dir, self.save_name)
        utils.save_df(save_path, df)

        df = utils.load_df(save_path)
        df = df.fillna('NA_NO')
        utils.save_df(save_path, df)