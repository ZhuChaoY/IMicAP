from collectors import ChEBICollector



def demo_chebi_collect():
    print("Start demo ChEBI scan...")
    root_dir = "../../result/collect_result/DB_ChEBI/"

    save_dir = f'{root_dir}DB_ChEBI/Ontology/'
    ChEBICollector.init_ontology_collect(save_dir)

    save_dir = f'{root_dir}DB_ChEBI/SDF/'
    ChEBICollector.init_SDF_collect(save_dir)
    print("Demo finished!")


if __name__ == '__main__':
    demo_chebi_collect()
