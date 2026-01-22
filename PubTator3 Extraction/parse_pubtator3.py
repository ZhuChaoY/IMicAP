import re
import time
import random
import numpy as np
import pandas as pd
import xml.dom.minidom as xdm
from os import listdir
from os.path import exists
from utils import check_dir, myjson
from utils import get_elements, get_attribute
from utils import check_min_distance, process_paragraph
from utils import count2df, doi2journal, link_jcr_journals



class Parse_PubTator3():
    def __init__(self):
        """
        Extract quads from PubTator3.
        Download and unzip data from:
        https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator3/
        3 entity types:
            GM: Gut Microbe
            DI: Disease
            SM: Small Molecule
        2 relation types:
            GM2DI (Activate, Inhibit) 
            GM2SM (Produce, Consume, Sensitive-to, Resistant-to)
        """
        
        print('\n=======  Parse PubTator3  =======')
        self.ets = ['GM', 'DI', 'SM']
        self.rts = ['GM2DI', 'GM2SM']
        
        self.all_dir = check_dir('pubtator3_all/')
        
        self.load_valid_entities()
        self.screen_gm_pubtator3()
        
        self.parse_pubtator3()
        self.merge_split_pubtator3()
        self.generate_re_datasets()
        self.split_re_datasets(train_ratio = 0.8)
        ###Run relation_extraction/run_gmkg_re.py first.
        self.get_quad_infor()
        self.generate_used_quads(pos_MIN = 0.6000, neg_MAX = 0.4000   ,
                                 m_n = 0.800, b1_n = 5   , b2_n = 50  ,
                                 m_y = 0.950, b1_y = 1990, b2_y = 2020,
                                 m_j = 0.900, b1_j = 1.0 , b2_j = 10.0)

    
    def load_valid_entities(self):
        """
        Load valid entities.
        Inputs:
            ../entities/GM/pt3_dict.json (GM entity)
            ../entities/DI&SM/pt3_dict.json (DI and SM entities)
        """
                
        gm_dict = myjson('../entities/GM/pt3_dict')
        dism_dict = myjson('../entities/DI&SM/pt3_dict')
        self.pt3_dict = {'GM': gm_dict, 'DI': {}, 'SM': {}}
        for x, y in dism_dict.items():
            self.pt3_dict[y[: 2]][x] = y
            
        fo_E = myjson('../entities/GM/E_dict')
        pm_E = myjson('../entities/DI&SM/E_dict')
        self.name_dict = {}        
        for et in self.ets:           
            e_dict = fo_E if et == 'GM' else pm_E[et]
            for e in self.pt3_dict[et].values():
                self.name_dict[e] = e_dict[e]['name']
        
        for et in self.ets:
            print('--{:2} : {:4}'.format(et, len(self.pt3_dict[et])))
        
        
    def screen_gm_pubtator3(self):
        """
        Screen PubTator3 articles with gut microbe entities.
        Process xml file as text file could be much faster.
        Inputs:
            pubtator3_all/BioCXML.0 -> 9/output/BioCXML/XXXXX.BioC.XML 
            [Self download from https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator3/]
        Ouputs:
            pubtator3_all/count_dict.json
            pubtator3_gm/split_pubtator3/0 -> 9/XXXXX.xml
            pubtator3_gm/split_pubtator3/count_dict.json
        """  

        print('\n>>  Screen articles with gut microbe entities.')
        
        self.gm_dir = check_dir('pubtator3_gm/')
                        
        p_all = self.all_dir + 'count_dict.json'
        count_all = myjson(p_all) if exists(p_all) else {}
        p_gm = self.gm_dir + 'split_pubtator3/count_dict.json'
        count_gm = myjson(p_gm) if exists(p_gm) else {}
        if len(count_all) == 10 and len(count_gm) == 10:
            print('--PubTator3 : {:8} -> {:8}'.format(sum(count_all.values()),
                                                      sum(count_gm.values())))
        else:
            gms = {x.split('|')[1] for x in self.pt3_dict['GM'].keys()}
            for k in range(10):
                k = str(k)
                if k in count_all and k in count_gm:
                    continue

                all_dir = '{}BioCXML.{}/output/BioCXML/'. \
                    format(self.all_dir, k)
                gm_dir = check_dir('{}split_pubtator3/{}/'. \
                                   format(self.gm_dir, k))
                old_files = listdir(all_dir)
                n_file, n_all, n_gm = len(old_files), 0, 0
                t0 = time.time()
                for i in range(n_file):
                    old_file = old_files[i]
                    lines = []
                    with open(all_dir + old_file, encoding = 'utf-8') as file:
                        for line in file:                    
                            if line[: 5] == '<?xml' or \
                                line == '</collection>\n':
                                lines.append(line)
                            else:
                                n_all += 1
                                target = '[0-9]*</infon><infon key="type">Sp'
                                sps = re.findall(target, line)
                                taxids = {sp.split('<')[0] for sp in sps}
                                if taxids & gms:
                                    n_gm += 1
                                    lines.append(line)
                    if len(lines) > 2:
                        new_file = '{:0>6}.xml'.format(old_file.split('.')[0])
                        with open(gm_dir + new_file, 'w',
                                  encoding = 'utf-8') as file:
                            file.writelines(lines)
                    if (i + 1) % 1000 == 0 or (i + 1) == n_file:
                        print('--pt3_{} : [{:5} | {:5}] ({:5.2f}mins)'. \
                             format(k, i + 1, n_file, (time.time() - t0) / 60)) 
                
                count_all[k], count_gm[k] = n_all, n_gm
                myjson(p_all, count_all)
                myjson(p_gm, count_gm)
                print('--pt3_{} : {} -> {}'.format(k, n_all, n_gm))
            

    def parse_pubtator3(self):
        """
        Parse PubTator3 xml file to extract paragraphs:
        (1) re_input: input paragraphs for relation extraction task.
        (2) pmid_infor: year and journal information of screened article pmids.
        Iuputs:
            pubtator3_gm/split_pubtator3/0 -> 9/XXXXX.xml
        Outputs:
            journals/doi_dict.json
            pubtator3_gm/split_results/result_0 -> 9.json
        """
        
        print('\n>>  Parse paragraphs from screened articles.')
        
        split_dir = check_dir(self.gm_dir + 'split_results/')
        
        p_gm = self.gm_dir + 'split_pubtator3/count_dict.json'
        count_gm = myjson(p_gm) if exists(p_gm) else {}
        p_result = split_dir + 'count_dict.json'
        count_result = myjson(p_result) if exists(p_result) else {}
        if len(count_gm) == 10 and len(count_result) == 10:
            print('--PubTator3 : {:8} -> {:8}'.format(sum(count_gm.values()),
                                                   sum(count_result.values())))
        else:            
            p_doi = 'journals/doi_dict.json'
            self.doi_dict = myjson(p_doi) if exists(p_doi) else {}
            for k in range(10):
                k = str(k)
                if k in count_gm and k in count_result:
                    continue
                             
                results = {}
                data_dir = '{}split_pubtator3/{}/'.format(self.gm_dir, k)
                xmls = listdir(data_dir)
                n_xml = len(xmls)
                t0 = time.time()
                for i in range(n_xml):
                    result = {}
                    gm_xml = xdm.parse(data_dir + xmls[i]).documentElement
                    for article in get_elements(gm_xml, 'document'):
                        pmid = get_elements(article, 'id', '-')
                        if pmid == '-':
                            continue
                        passages = get_elements(article, 'passage')
                        re_input = self._get_re_input(passages[1: ])
                        if re_input != []:
                            pmid_infor = self._get_pmid_infor(passages[0])
                            result[pmid] = {'pmid_infor': pmid_infor,
                                            're_input': re_input}
                    results.update(result)
                    if (i + 1) % 1000 == 0 or (i + 1) == n_xml:
                        myjson('{}result_{}'.format(split_dir, k), results)
                        myjson(p_doi, self.doi_dict)
                        print('--pt3_{} : [{:4} | {:4}] ({:5.2f}mins)'. \
                              format(k, i + 1, n_xml, (time.time() - t0) / 60))
                
                count_result[k] = len(results)
                myjson(p_result, count_result)
                print('--pt3_{} : {} -> {}'. \
                    format(k, count_gm[k], count_result[k]))
                        
                    
    def merge_split_pubtator3(self):
        """
        Merge 10 split pubtator3 results of valid DI and SM entities.
        Inputs:
            pubtator3_gm/split_results/result_0 -> 9.json
            journals/jcr-if-2023
        Outputs:
            pubtator3_gm/pmid_infor.json, count_dict.json, count_result.xlsx
            pubtator3_gm/re_input_gm2(di, sm)_(0, 1).json        
            journals/j2jcr
        """

        print('\n>>  Merge 10 split pubtator3 results '
              'of valid DI and SM entities.')

        p_count = self.gm_dir + 'count_dict.json'
        if not exists(p_count):
            count_dict = {'year':   {}, 'journal' : {}, 
                          'entity': {}, 'relation': {}}
            for gm in self.pt3_dict['GM'].values():
                count_dict['entity'][gm] = 0
            pmid_infor = {}
            for k in range(10):
                if k % 5 == 0:
                    re_inputs = {'DI': {}, 'SM': {}}
                for pmid, dic in myjson('{}split_results/result_{}'. \
                                        format(self.gm_dir, k)).items():
                    add_flag = False
                    for re_input in dic['re_input']:
                        splits = re_input.split('[SEP]')
                        h, t = splits[0].split('&')
                        et = 'DI' if t.split('|')[0] == 'Disease' else 'SM'
                        e_dict = self.pt3_dict[et]
                        if t not in e_dict:
                            continue
                        add_flag = True
                        r, t = 'GM2' + et, e_dict[t]
                        pair = h + '&' + t
                        count_dict['entity'][h] = \
                            count_dict['entity'].get(h, 0) + 1
                        count_dict['entity'][t] = \
                            count_dict['entity'].get(t, 0) + 1
                        count_dict['relation'][r] = \
                            count_dict['relation'].get(r, 0) + 1
                        re_inputs[et][pair] = \
                            re_inputs[et].get(pair, []) + \
                                [pmid + '|SEP|' + '[SEP]'.join(splits[1: ])]
                    if add_flag:
                        _pmid_infor = dic['pmid_infor']
                        y, j = _pmid_infor.split('|')
                        count_dict['year'][y] = \
                            count_dict['year'].get(y, 0) + 1
                        count_dict['journal'][j] = \
                            count_dict['journal'].get(j, 0) + 1
                        pmid_infor[pmid] = _pmid_infor
                    
                myjson(p_count, count_dict)    
                myjson(self.gm_dir + 'pmid_infor', pmid_infor)
                if k % 5 == 4:
                    for et in ['DI', 'SM']:
                        myjson(self.gm_dir + 're_input_gm2{}_{}'. \
                               format(et.lower(), k // 5), re_inputs[et])
        else:
            count_dict = myjson(p_count)
        
        p_count_xlsx = self.gm_dir + 'count_result.xlsx'
        if not exists(p_count_xlsx):
            writer = pd.ExcelWriter(p_count_xlsx)
            
            for key in ['year', 'journal', 'relation', 'entity']:
                df = count2df(count_dict[key], key)
                es = list(df[key])
                if key != 'entity':
                    if key == 'journal':
                        j2if = myjson('journals/jcr-if-2023')
                        j2jcr = link_jcr_journals(es, list(j2if.keys()))
                        myjson('journals/j2jcr', j2jcr)
                        jcrs = [j2jcr[j] if j in j2jcr else '' for j in es]
                        ifs = [j2if[jcr] if jcr != '' else '' for jcr in jcrs]
                        df.insert(1, 'JCR', jcrs)
                        df.insert(2, 'IF-2023', ifs)
                        df = df.sort_values(by = ['count', 'IF-2023'],
                                            ascending = False)
                    df.to_excel(writer, key, index = 0)
                else:                
                    df.insert(1, 'name', [self.name_dict[e] for e in es])
                    et_dict = {et: [] for et in self.ets}
                    for i, e in enumerate(es):
                        et_dict[e[: 2]].append(i)
                    for et in self.ets:
                        _df = df.iloc[et_dict[et], :]
                        _df.to_excel(writer, 'entity-' + et, index = 0)
                
            writer.save()
            writer.close()
            
        writer = pd.ExcelFile(p_count_xlsx)
        for key in ['year', 'journal', 'relation']:
            df = pd.read_excel(writer, key)
            es = list(df[key])
            counts = list(df['count'])
            n, N = len(df), sum(counts)
            if key == 'year':
                if '-' in es:
                    es.remove('-')
                print('++++article : {:6}'.format(N))
                print(' +++{:7} : {:6} ({} -> {})'. \
                      format(key, n, min(es), max(es)))
            elif key == 'journal':
                jcr_df = df.dropna(subset = ['JCR'])
                print(' +++{:7} : {:6}'.format(key, n))
                print('  ++jcr     : {:6} ({:5.2f}%)\n'. \
                      format(len(jcr_df), sum(jcr_df['count']) / N * 100))
            elif key == 'relation':
                print(' +++re_input : {:7}'.format(N))
                for r, count in zip(es, counts):
                    print('  ++{:8} : {:7}'.format(r, count))
                    
        print()
        for et in self.ets:
            df = pd.read_excel(writer, 'entity-' + et)
            print('  ++{:2} : {:4} '.format(et, len(df)))

            
    def generate_re_datasets(self, n_para = 3000):
        """
        Generate relation extraction annotation datasets.
        Inputs:
            pubtator3_gm/re_input_gm2(di, sm)_(0, 1).json 
        Outputs:
            relation_extraction/annotation_datasets/GM2(DI, SM).xlsx
            relation_extraction/annotation_datasets/GM2(DI, SM)-test.xlsx
        """
        
        print('\n>>  Generate relation extraction annotation datasets.')
        self.re_dir = check_dir('relation_extraction/')
        
        re_dir = check_dir(self.re_dir + 'annotation_datasets/')
        for et in ['DI', 'SM']:
            p_re_1 = '{}GM2{}.xlsx'.format(re_dir, et)
            p_re_2 = '{}GM2{}-test.xlsx'.format(re_dir, et)
            if not exists(p_re_1) or not exists(p_re_2):            
                gm_dict = {}
                for k in range(2):
                    re_input = myjson(self.gm_dir + 're_input_gm2{}_{}'. \
                                      format(et.lower(), k))
                    for pair, y in re_input.items():
                        gm = pair.split('&')[0]
                        gm_dict[gm] = gm_dict.get(gm, []) + \
                            random.sample(y, min(10, len(y)))
                    
                paras = []
                for y in gm_dict.values():
                    paras.extend(random.sample(y, min(n_para // 200, len(y))))
                if len(paras) > n_para:
                    paras = random.sample(paras, n_para)
                gms = list(gm_dict.keys())
                while len(paras) < n_para:
                    paras.append(random.choice(gm_dict[random.choice(gms)]))
                    paras = sorted(set(paras))
                
                prompt = 'In the following paragraph, we have marked a gut ' \
                         'microbe entity (between @gm$ and @/gm$ symbols) and ' 
                if et == 'DI':
                    prompt += \
            'a disease entity (between @di$ and @/di$ symbols). If you are a' \
            ' biomedical expert, please tell me which relationship exists ' \
            'between these two entities with no explanation. Candidate ' \
            'relationships include: \n0 means there is no association.\n1 ' \
            'indicates gut microbe activates disease.\n2 indicates gut ' \
            'microbe inhibits disease.'
                elif et == 'SM':
                    prompt += \
            'a small molecule entity (between @sm$ and @/sm$ symbols). If ' \
            'you are a biomedical expert, please tell me which relationship' \
            ' exists between these two entities with no explanation. ' \
            'Candidate relationships include: \n0 means there is no ' \
            'association.\n1 indicates gut microbe produces small molecule' \
            '.\n2 indicates gut microbe consumes small molecule.\n3 ' \
            'indicates gut microbe is sensitive to small molecule.\n4 ' \
            'indicates gut microbe is resistant to small molecule.'
                prompt += '\nParagraph:\n'
                
                paras = [para.split('|SEP|')[1] for para in paras]
                paras_1 = [prompt + para for para in paras[: -100]]
                df_1 = pd.DataFrame({'gpt-4': [''] * (n_para - 100),
                                     'gpt-4o': [''] * (n_para - 100),
                                     'diff': [''] * (n_para - 100),
                                     'paragraph': paras_1})
                df_1 = df_1.sort_values(by = 'paragraph')
                df_1.to_excel(p_re_1, index = 0)
                paras_2 = paras[-100: ]
                df_2 = pd.DataFrame({'label': [''] * 100,
                                     'paragraph': paras_2})
                df_2 = df_2.sort_values(by = 'paragraph')
                df_2.to_excel(p_re_2, index = 0)
            else: 
                df_1 = pd.read_excel(p_re_1)
                df_2 = pd.read_excel(p_re_2)
                
            n_1 = len(df_1)
            print('++GM2{}      : ({:4}, {:4}) -> {:4} | {:4}'. \
                  format(et, n_1 - sum(df_1['gpt-4'].isna()),
                             n_1 - sum(df_1['gpt-4o'].isna()),
                             list(df_1['diff']).count(0), n_1))
            n_2 = len(df_2)
            print('++GM2{}-test : {:3} | {:3}'. \
                  format(et, n_2 - sum(df_2['label'].isna()), n_2))   
            
            
    def split_re_datasets(self, train_ratio):
        """
        Split relation extraction datasets into train, dev and test datasets.
        Inputs:
            relation_extraction/annotation_datasets/GM2(DI, SM).xlsx
            relation_extraction/annotation_datasets/GM2(DI, SM)-test.xlsx
        Outputs:
            relation_extraction/re_datasets/GM2(DI, SM)/(train, dev, test).tsv
        """
        
        print('\n>>  Split relation extraction datasets after labelling.')
        
        print('           ALL   0    1    2    3    4')
        for et in ['DI', 'SM']:
            n_label = 3 if et == 'DI' else 5
            re_dir = check_dir(self.re_dir + 're_datasets/GM2{}/'.format(et))
            p_train, p_dev, p_test = \
                re_dir + 'train.tsv', re_dir + 'dev.tsv', re_dir + 'test.tsv'
            if not exists(p_train) or not exists(p_dev) or not exists(p_test): 
                df = pd.read_excel(self.re_dir + 'annotation_datasets' \
                                                 '/GM2{}.xlsx'.format(et))
                if sum(df['diff'].isna()) != 0:
                    print('++{} : Have not labelled all paragraphs!'. \
                          format('GM2' + et))
                    continue
                df = df[df['diff'].isin([0])]
                df['label'] = df['gpt-4o']
                df['paragraph'] = [x.split('\nParagraph:\n')[1].strip() 
                                   for x in df['paragraph']]
                df = df.drop(labels = ['gpt-4', 'gpt-4o', 'diff'], axis = 1) 
                train, dev = [], []
                for i in range(n_label):                
                    _df = df[df['label'].isin([i])]                
                    train.append(_df.sample(frac = train_ratio))
                    dev.append(_df[~_df.index.isin(train[-1].index)])
                train = pd.concat(train).sort_values(by = 'paragraph')
                train.to_csv(p_train, index = 0, sep = '\t')
                dev = pd.concat(dev).sort_values(by = 'paragraph')
                dev.to_csv(p_dev, index = 0, sep = '\t')
                
                test = pd.read_excel(self.re_dir + 'annotation_datasets' \
                                                 '/GM2{}-test.xlsx'.format(et))
                test = test[['paragraph', 'label']]
                test.to_csv(p_test, index = 0, sep = '\t')
                
            else:
                train = pd.read_csv(p_train, sep = '\t')
                dev = pd.read_csv(p_dev, sep = '\t')
                test = pd.read_csv(p_test, sep = '\t')
            
            tr = [list(train['label']).count(x) for x in range(n_label)]
            de = [list(dev['label']).count(x) for x in range(n_label)]
            te = [list(test['label']).count(x) for x in range(n_label)]
            n_tr, n_de, n_te = sum(tr), sum(de), sum(te)
            print('++GM2{:2} : {:4}'.format(et, n_tr + n_de), end = '')
            for i in range(n_label):
                print(' {:4}'.format(tr[i] + de[i]), end = '')
            print('\n +train : {:4}'.format(n_tr), end = '')
            for i in range(n_label):
                print(' {:4}'.format(tr[i]), end = '')
            print('\n +dev   : {:4}'.format(n_de), end = '')
            for i in range(n_label):
                print(' {:4}'.format(de[i]), end = '')
            print('\n +test  : {:4}'.format(n_te), end = '')
            for i in range(n_label):
                print(' {:4}'.format(te[i]), end = '')
            print()
         
    
    def get_quad_infor(self):
        """
        [ Run relation_extraction/run_gmkg_re.py first. ]
        Get quad information from relation extraction results.
        Inputs:
            relation_extraction/re_datatsets/GM2(DI, SM)/
                                plm_name+smapling_type/re_result.json
            pubtator3_gm/pmid_infor.json
            journals/j2jcr.json, jcr-if-2023.json
        Outputs:
            used_quads/GM2(DI, SM)_infor.json
        """
    
        print('\n>>  Get quad information from extraction results.')
    
        self.quad_dir = check_dir('used_quads/')
        
        print('            ALL    0     1     2     3     4')
        for et in ['DI', 'SM']:
            n_label = 3 if et == 'DI' else 5
            p_infor = '{}GM2{}_infor.json'.format(self.quad_dir, et)
            if not exists(p_infor):            
                pmid_infor = myjson('pubtator3_gm/pmid_infor')
                j2jcr = myjson('journals/j2jcr')
                jcr2if = myjson('journals/jcr-if-2023')
                
                if et == 'DI':
                    model_name = 'pubmedbert+none'
                else:
                    model_name = 'biolinkbert+sub-up'
                re_results = myjson('relation_extraction/re_datasets/GM2{}/' \
                                    '{}/re_result'.format(et, model_name))
                
                pair_infor = {}
                for pair, re_result in re_results.items():
                    ys, js, _ss = [], [], []
                    pmid_dict = {}
                    for pmid, s in re_result:                    
                        pmid_dict[pmid] = pmid_dict.get(pmid, []) + [s]
                    for pmid, s in pmid_dict.items():
                        y, _j = pmid_infor[pmid].split('|')
                        j = jcr2if[j2jcr[_j]] if _j in j2jcr else 0.0
                        ys.append(min(int(y), 2024) if y != '-' else 1896)
                        js.append(j)
                        _ss.append([x for x in np.mean(s, 0)])
                    
                    split_ss = [[i] for i in range(n_label)]
                    for i, j in enumerate(np.argmax(_ss, 1)):
                        split_ss[j].append(_ss[i][j])
                    r = sorted(split_ss, key = lambda x: (-len(x),
                               -np.mean(x[1: ]) if x else 0.0))[0][0]
                    ss = [round(_s[r], 4) for _s in _ss]
                    negs = {j: round(np.mean([_s[j] for _s in _ss]), 4) 
                            for j in range(1, n_label) if j != r}
                
                    pair_infor[pair] = {'r': r, 'ys': ys, 'js': js,
                                        'ss': ss, 'negs': negs}
                myjson(p_infor, pair_infor)
                
            pair_infor = myjson(p_infor)
            rs = [y['r'] for y in pair_infor.values()]
            print('++GM2{} : {:6} {:5} {:5} {:5}'.format(et, len(pair_infor),
                   rs.count(0), rs.count(1), rs.count(2)), end = '')
            if et == 'SM':
                print(' {:5} {:5}'.format(rs.count(3), rs.count(4))) 
            else:
                print('')

    
    def generate_used_quads(self, pos_MIN, neg_MAX, m_n, b1_n, b2_n,
                                  m_y, b1_y, b2_y, m_j, b1_j, b2_j):
        """
        Generate used quads from extracted information:
        Calculate confidence score (cs) from:
        (1) occurrence frequency (n)
        (2) publish years (ys)
        (3) publish journal impact factors (js)
        (4) relation extraction scores (ss)
        Normalize positive confidence score to [0.6000, 0.9999].
        Normalize negative confidence score to [0.0001, 0.4000].
        Inputs:
            used_quads/GM2(DI, SM)_infor.json
        Outputs:
            used_quads/GM2(DI, SM).tsv
            used_quads/GM2(DI, SM)_neg.tsv
            used_quads/GM2(DI, SM)_name.xlsx
        """
    
        print('\n>>  Generate used quads from extracted information.')
        
        trans_n = lambda n: (1.0 - m_n) / (b2_n - b1_n) * \
                            (min(max(n, b1_n), b2_n) - b1_n) + m_n
        trans_y = lambda y: (1.0 - m_y) / (b2_y - b1_y) * \
                            (min(max(y, b1_y), b2_y) - b1_y) + m_y
        trans_j = lambda j: (1.0 - m_j) / (b2_j - b1_j) * \
                            (min(max(j, b1_j), b2_j) - b1_j) + m_j             
                            
        print('                ALL     1      2      3      4    MEAN')
        for et in ['DI', 'SM']:
            p_pos = '{}GM2{}.tsv'.format(self.quad_dir, et)
            p_neg = '{}GM2{}_neg.tsv'.format(self.quad_dir, et)
            p_name = '{}GM2{}_name.xlsx'.format(self.quad_dir, et)
            if et == 'DI':
                r_dict = {1: 'GM2DI_Activate',     2: 'GM2DI_Inhibit'}
            elif et == 'SM':
                r_dict = {1: 'GM2SM_Produce',      2: 'GM2SM_Consume', 
                          3: 'GM2SM_Sensitive-to', 4: 'GM2SM_Resistant-to'}
            if not exists(p_pos) or not exists(p_neg) or not exists(p_name):
                pair_infor = myjson('{}GM2{}_infor'.format(self.quad_dir, et))
                pos_quads, neg_quads = [], []
                for pair, infor in pair_infor.items():
                    h, t = pair.split('&')
                    r = infor['r']
                    if r != 0:
                        ys, js, ss = infor['ys'], infor['js'], infor['ss']
                        n = len(ys)
                        k_n = trans_n(n)
                        cs = 0.0
                        for y, j, s in zip(ys, js, ss):
                            k_y = trans_y(y)
                            k_j = trans_j(j)
                            cs += k_y * k_j * s
                        cs = round(k_n * cs / n, 4)
                        pos_quads.append([h, r_dict[int(r)], t, cs])
                    
                    for j, s in infor['negs'].items():
                        neg_quads.append([h, r_dict[int(j)], t, s])
                        
                pos_css = [quad[-1] for quad in pos_quads]
                pos_mi, pos_ma = min(pos_css), max(pos_css)
                pos_trans = lambda s: round((s - pos_mi) / (pos_ma - pos_mi) *\
                                             (0.9999 - pos_MIN) + pos_MIN, 4)
                pos_quads = [quad[: 3] + [pos_trans(quad[-1])] 
                             for quad in pos_quads]
                
                neg_css = [quad[-1] for quad in neg_quads]
                neg_mi, neg_ma = min(neg_css), max(neg_css)
                neg_trans = lambda s: round((s - neg_mi) / (neg_ma - neg_mi) *\
                                             (neg_MAX - 0.0001) + 0.0001, 4)
                neg_quads = [quad[: 3] + [neg_trans(quad[-1])] 
                             for quad in neg_quads]
                
                cols = ['Head Entity', 'Relation', 'Tail Entity',
                        'Confidence Score']
                pos_df = pd.DataFrame(pos_quads, columns = cols)
                pos_df = pos_df.drop_duplicates()
                pos_df = pos_df.sort_values(by = cols[-1: ] + cols[: -1], 
                                            ascending = False)
                pos_df.to_csv(p_pos, sep = '\t', index = 0)
                neg_df = pd.DataFrame(neg_quads, columns = cols)
                neg_df = neg_df.drop_duplicates()
                neg_df = neg_df.sort_values(by = cols[-1: ] + cols[: -1], 
                                            ascending = False)
                neg_df.to_csv(p_neg, sep = '\t', index = 0)
                
                dics = [myjson('{}re_input_gm2{}_{}'. \
                               format(self.gm_dir, et, k)) for k in range(2)]
                PMID = []
                for i in range(len(pos_df)):
                    line = list(pos_df.iloc[i, :])
                    h, t = line[0], line[2]
                    key = h + '&' + t
                    pmids = []
                    for dic in dics:
                        if key in dic:
                            pmids.extend([infor.split('|SEP|')[0] 
                                          for infor in dic[key]])
                    PMID.append(';'.join(sorted(set(pmids), reverse = True)))
                
                pos_df[cols[0]] = [self.name_dict[h] for h in pos_df[cols[0]]]
                pos_df[cols[2]] = [self.name_dict[t] for t in pos_df[cols[2]]]
                pos_df['PMIDs'] = PMID
                pos_df.to_excel(p_name, index = 0)
                
            pos_df = pd.read_csv(p_pos, sep = '\t')            
            rs = list(pos_df['Relation'])
            print('++GM2{}-pos : {:6}'.format(et, len(rs)), end = '')
            for j in range(1, 3 if et == 'DI' else 5):
                print(' {:6}'.format(rs.count(r_dict[j])), end = '')
            print('{}{:6.4f}'.format(' ' * 15 if et == 'DI' else ' ' * 1,
                                      np.mean(pos_df['Confidence Score'])))
            
            neg_df = pd.read_csv(p_neg, sep = '\t')            
            rs = list(neg_df['Relation'])
            print('++GM2{}-neg : {:6}'.format(et, len(rs)), end = '')
            for j in range(1, 3 if et == 'DI' else 5):
                print(' {:6}'.format(rs.count(r_dict[j])), end = '')
            print('{}{:6.4f}'.format(' ' * 15 if et == 'DI' else ' ' * 1,
                                      np.mean(neg_df['Confidence Score'])))

    
    def _get_re_input(self, passages, max_distance = 384, #old version: 512
                      min_length = 128, max_length = 2048):
        """Get input paragraphs for relation extraction task."""
                
        re_input = []
        for passage in passages:
            st1 = get_attribute(passage, 'section_type')
            if st1 not in {'-', 'ABSTRACT', 'INTRO', 'METHODS',
                           'RESULTS', 'DISCUSS'}:
                continue
            if st1 == '-':
                st2 = get_attribute(passage, 'type')
                if st2 not in {'-', 'abstract'}:
                    continue
                
            off = int(get_elements(passage, 'offset', -1))
            if off == -1:
                continue
            
            e_dict = self.pt3_dict['GM']
            h2loc, t2loc = {}, {}
            for ann in get_elements(passage, 'annotation'):
                typ = get_attribute(ann, 'type')
                if typ not in {'Chemical', 'Disease', 'Species'}:
                    continue
                ide = get_attribute(ann, 'identifier')
                if ide == '-':
                    continue
                if typ == 'Species':
                    _es = [typ + '|' + taxid for taxid in ide.split(';')]
                    es = [e_dict[e] for e in _es if e in e_dict]
                    if len(es) == 1:
                        e = es[0]
                    else:
                        continue
                else:
                    e = typ + '|' + ide                    
                loc = get_elements(ann, 'location', [])[0]
                start = int(loc.getAttribute('offset')) - off
                end = start + int(loc.getAttribute('length'))
                if e[: 2] == 'GM':
                    h2loc[e] = h2loc.get(e, []) + [(start, end)]
                else:
                    t2loc[e] = t2loc.get(e, []) + [(start, end)]
                
            if h2loc == {} or t2loc == {}:
                continue
            _para = get_elements(passage, 'text', '-')
            for h in sorted(h2loc.keys()):
                typ1, loc1 = 'gm', h2loc[h]
                for t in sorted(t2loc.keys()):
                    typ2 = 'di' if t.split('|')[0] == 'Disease' else 'sm'
                    loc2 = t2loc[t]
                    if not check_min_distance(loc1, loc2, max_distance):
                        continue
                    para = process_paragraph(_para, loc1, typ1, loc2, typ2)
                    if min_length <= len(para) <= max_length:
                        re_input.append(h + '&' + t + '[SEP]' + para)
        
        return re_input
                    
    
    def _get_pmid_infor(self, infor):
        """Get year and journal information of screened gm_pmids."""
        
        year = get_attribute(infor, 'year').strip()[: 4]
        doi = get_attribute(infor, 'article-id_doi')
        if doi != '-':            
            j_doi = doi.split('/')[0][3: ]
            if j_doi not in self.doi_dict:
                j = doi2journal(doi)
                if j != '-':
                    self.doi_dict[j_doi] = j
                else:
                    j = 'doi:' + j_doi
            else:
                j = self.doi_dict[j_doi]
        else:
            j = get_attribute(infor, 'journal')
        if j != '-' and j[: 3] != 'doi':
            j = j.lower()
            j = j.split(';')[0]
            j = j.replace('.', '')
            j = j.split(' doi:')[0]
            j = j.split(' pii:')[0]
            j = re.sub('[ ]?[0-9]{4}.*$', '', j)
            j = re.sub('[ ]?\(.+$', '', j)
            j = j.strip(' (')
            if re.findall('[a-z]', j) == []:
                j = '-'
        pmid_infor = year + '|' + j
        
        return pmid_infor



if __name__ == '__main__':

    obj = Parse_PubTator3()