import json
import os
import requests
import utils.my_df_function as utils


class UniProtDataSpider:

    def __init__(self, now_time, ref_path, root_dir, limit_cycle):
        self.date_time = now_time
        self.ref_path = ref_path
        self.root_dir = root_dir
        utils.create_dir(self.root_dir)
        self.limit_cycle = limit_cycle

    def tsv_spider_process(self, li_shan_species_tax_id, date_time, save_dir_all, record_dir):
        species_tax_id = li_shan_species_tax_id.replace('lishan_', '')
        print(species_tax_id)
        for_download_species_id = f'(taxonomy_id:{species_tax_id})'
        download_url = 'https://rest.uniprot.org/uniprotkb/search?compressed=false&fields=accession%2Creviewed%2Cid%2Cprotein_name%2Cgene_names%2Corganism_name%2Clength%2Cgene_oln%2Cgene_orf%2Cgene_primary%2Cgene_synonym%2Corganism_id%2Cxref_proteomes%2Clineage%2Clineage_ids%2Cvirus_hosts%2Ccc_alternative_products%2Ccc_mass_spectrometry%2Cft_variant%2Ccc_sequence_caution%2Cft_conflict%2Cft_unsure%2Cabsorption%2Cft_act_site%2Cft_binding%2Ccc_catalytic_activity%2Ccc_cofactor%2Cft_dna_bind%2Cec%2Ccc_activity_regulation%2Ccc_function%2Ckinetics%2Ccc_pathway%2Cph_dependence%2Credox_potential%2Crhea%2Cft_site%2Ctemp_dependence%2Ccc_interaction%2Ccc_subunit%2Ccc_developmental_stage%2Ccc_induction%2Ccc_tissue_specificity%2Cgo_p%2Cgo_c%2Cgo%2Cgo_f%2Cgo_id%2Ccc_allergen%2Ccc_biotechnology%2Ccc_disruption_phenotype%2Ccc_disease%2Cft_mutagen%2Ccc_pharmaceutical%2Ccc_toxic_dose%2Cft_intramem%2Ccc_subcellular_location%2Cft_topo_dom%2Cft_transmem%2Cft_chain%2Cft_crosslnk%2Cft_disulfid%2Cft_carbohyd%2Cft_init_met%2Cft_lipid%2Cft_mod_res%2Cft_peptide%2Ccc_ptm%2Cft_propep%2Cft_signal%2Cft_transit%2Clit_pubmed_id%2Clit_doi_id%2Cdate_created%2Cversion%2Cft_coiled%2Cft_compbias%2Ccc_domain%2Cft_domain%2Cft_motif%2Cprotein_families%2Cft_region%2Cft_repeat%2Cft_zn_fing%2Ccc_similarity%2Cxref_biogrid%2Cxref_corum%2Cxref_complexportal%2Cxref_dip%2Cxref_elm%2Cxref_intact%2Cxref_mint%2Cxref_string%2Cxref_bindingdb%2Cxref_chembl%2Cxref_drugbank%2Cxref_drugcentral%2Cxref_guidetopharmacology%2Cxref_swisslipids%2Cxref_ccds%2Cxref_embl%2Cxref_generif%2Cxref_refseq%2Cxref_pir%2Cxref_allergome%2Cxref_cazy%2Cxref_esther%2Cxref_imgt_gene-db%2Cxref_merops%2Cxref_moondb%2Cxref_moonprot%2Cxref_peroxibase%2Cxref_rebase%2Cxref_tcdb%2Cxref_unilectin%2Cxref_carbonyldb%2Cxref_depod%2Cxref_glyconnect%2Cxref_glycosmos%2Cxref_glygen%2Cxref_metosite%2Cxref_phosphositeplus%2Cxref_swisspalm%2Cxref_unicarbkb%2Cxref_iptmnet%2Cxref_alzforum%2Cxref_biomuta%2Cxref_dmdm%2Cxref_dbsnp%2Cxref_antibodypedia%2Cxref_ensembl%2Cxref_ensemblbacteria%2Cxref_ensemblfungi%2Cxref_ensemblmetazoa%2Cxref_ensemblplants%2Cxref_ensemblprotists%2Cxref_geneid%2Cxref_gramene%2Cxref_kegg%2Cxref_mane-select%2Cxref_patric%2Cxref_ucsc%2Cxref_vectorbase%2Cxref_wbparasite%2Cxref_agr%2Cxref_hgnc%2Cxref_pharmgkb%2Cxref_cgd%2Cxref_ctd%2Cxref_disgenet%2Cxref_echobase%2Cxref_genecards%2Cxref_genereviews%2Cxref_hpa%2Cxref_ic4r%2Cxref_japonicusdb%2Cxref_legiolist%2Cxref_leproma%2Cxref_mgi%2Cxref_mim%2Cxref_malacards%2Cxref_niagads%2Cxref_opentargets%2Cxref_orphanet%2Cxref_pombase%2Cxref_pseudocap%2Cxref_rgd%2Cxref_sgd%2Cxref_tuberculist%2Cxref_veupathdb%2Cxref_vgnc%2Cxref_dictybase%2Cxref_euhcvdb%2Cxref_nextprot%2Cxref_brenda%2Cxref_biocyc%2Cxref_pathwaycommons%2Cxref_plantreactome%2Cxref_reactome%2Cxref_sabio-rk%2Cxref_signor%2Cxref_signalink%2Cxref_unipathway%2Cxref_biogrid-orcs%2Cxref_chitars%2Cxref_evolutionarytrace%2Cxref_genewiki%2Cxref_genomernai%2Cxref_orcid%2Cxref_pgenn%2Cxref_phi-base%2Cxref_pro%2Cxref_pharos%2Cxref_pubtator%2Cxref_rnact%2Cxref_emind%2Cxref_cdd%2Cxref_disprot%2Cxref_gene3d%2Cxref_hamap%2Cxref_ideal%2Cxref_interpro%2Cxref_ncbifam%2Cxref_panther%2Cxref_pirsf%2Cxref_prints%2Cxref_prosite%2Cxref_pfam%2Cxref_sfld%2Cxref_smart%2Cxref_supfam%2Cxref_cptac%2Cxref_massive%2Cxref_pride%2Cxref_paxdb%2Cxref_peptideatlas%2Cxref_promex%2Cxref_proteomicsdb%2Cxref_pumba%2Cxref_topdownproteomics%2Cxref_jpost%2Cxref_bgee%2Cxref_cleanex%2Cxref_expressionatlas%2Cxref_collectf%2Cxref_genetree%2Cxref_hogenom%2Cxref_inparanoid%2Cxref_oma%2Cxref_orthodb%2Cxref_phylomedb%2Cxref_treefam%2Cxref_eggnog&format=tsv&query=%28' + for_download_species_id + '%29&size=500'

        base_url = download_url
        save_dir = save_dir_all + '/UniProt_tsv_spider_result/' + li_shan_species_tax_id + '/'
        utils.create_dir(save_dir)

        error_dir = record_dir + f'{li_shan_species_tax_id}_tsv_spider_error/'
        utils.create_dir(error_dir)
        fail_save_path = error_dir + f'{li_shan_species_tax_id}_{date_time}.txt'

        retries = 5
        attempt_count = 0
        part_num = 1

        next_url = base_url
        while next_url:
            print(next_url)

            if self.limit_cycle:
                if part_num > self.limit_cycle:
                    break

            try:
                response = requests.get(next_url, timeout=(10, 60))

                data = response.text

                file_path = os.path.join(save_dir, li_shan_species_tax_id + f'_part{part_num}.tsv')

                with open(file_path, 'w+', encoding='utf-8') as f:
                    f.write(data)

                part_num += 1
                attempt_count = 0

                link_header = response.headers.get('Link')
                if link_header:
                    next_url = link_header.split(';')[0].strip('<>')
                else:
                    next_url = None

            except Exception as e:
                attempt_count += 1
                print(f'The {attempt_count}th fail: {e}')
                if attempt_count < retries:
                    utils.random_sleep(mu=0.68)
                else:
                    print('all attempt fail!')
                    with open(fail_save_path, 'a+') as f:
                        f.write(next_url)
                        f.write('\t')
                        f.write('\n')

            utils.random_sleep(mu=0.68)

    def json_spider_process(self, li_shan_species_tax_id, date_time, save_dir_all, record_dir):
        save_dir = save_dir_all + '/UniProt_json_spider_result/' + li_shan_species_tax_id + '/'
        utils.create_dir(save_dir)

        error_dir = record_dir + f'{li_shan_species_tax_id}_json_spider_error/'
        utils.create_dir(error_dir)
        fail_save_path = error_dir + f'{li_shan_species_tax_id}_{date_time}.txt'

        species_tax_id = li_shan_species_tax_id.replace('lishan_', '')
        print(species_tax_id)
        for_download_species_id = f'(taxonomy_id:{species_tax_id})'
        base_url = 'https://rest.uniprot.org/uniprotkb/search?format=json&query=%28%28' + for_download_species_id + '%29%29&size=500'

        retries = 5
        attempt = 0
        part_num = 1

        next_url = base_url
        while next_url:
            if self.limit_cycle:
                if part_num > self.limit_cycle:
                    break

            print(next_url)
            try:
                response = requests.get(next_url, timeout=(10, 60))

                data = response.json()

                file_path = os.path.join(save_dir, li_shan_species_tax_id + f'_part{part_num}.json')

                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4)

                part_num += 1
                attempt = 0

                link_header = response.headers.get('Link')
                if link_header:
                    next_url = link_header.split(';')[0].strip('<>')
                else:
                    next_url = None

            except Exception as e:
                attempt += 1
                print(f'The {attempt}th fail: {e}')
                if attempt < retries:
                    utils.random_sleep(mu=0.68)
                else:
                    print('all attempt fail!')

                    with open(fail_save_path, 'a+') as f:
                        f.write(next_url)
                        f.write('\t')
                        f.write('\n')

            utils.random_sleep(mu=0.68)

    def start_all(self):
        date_time = self.date_time

        path_reference = self.ref_path
        df_reference = utils.load_df(path_reference)
        target_columns = ['lishan_species_tax_id', 'species']
        df_reference = df_reference[target_columns]
        df_reference = df_reference.drop_duplicates()
        download_dict = df_reference.set_index('lishan_species_tax_id')['species'].to_dict()
        # print(len(df_reference))

        main_dir = self.root_dir
        save_dir = main_dir + 'Data/'
        utils.create_dir(save_dir)
        record_dir = main_dir + 'record/'
        utils.create_dir(record_dir)
        error_path = record_dir + 'Uniprot_collect_error.txt'

        for li_shan_species_id in download_dict:
            try:
                species_name = download_dict.get(li_shan_species_id)
                print(li_shan_species_id)
                print(species_name)

                self.tsv_spider_process(li_shan_species_tax_id=li_shan_species_id,
                                        date_time=date_time, save_dir_all=save_dir, record_dir=record_dir)

                self.json_spider_process(li_shan_species_tax_id=li_shan_species_id,
                                         date_time=date_time, save_dir_all=save_dir, record_dir=record_dir)
            except Exception as e:
                print(e)
                with open(error_path, 'a+') as f:
                    f.write(li_shan_species_id + '\t' + str(e) + '\n')
