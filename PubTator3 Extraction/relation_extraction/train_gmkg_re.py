import sys
import time
import torch
import random
import numpy as np
import pandas as pd
import torch.nn as nn
import sklearn.metrics as sm
from os.path import exists
from torch.optim import AdamW
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import get_linear_schedule_with_warmup
sys.path.append('../')
from utils import check_dir, myjson, mypickle



class GMKG_RE():
    def __init__(self, args):
        """
        Relation Extraction for Gut Microbes Knowledge Graph.
        3 classification classes for GM2DI and GM2SM relations:
            GM2DI (Activate, Inhibit) 
            GM2SM (Produce, Consume, Sensitive-to, Resistant-to)
        """
        
        self.args = dict(args._get_kwargs())
        for key, value in self.args.items():
            if type(value) == str:
                exec('self.{} = "{}"'.format(key, value))
            else:
                exec('self.{} = {}'.format(key, value))
            
        print('\n\n' + '==' * 4 + ' {} + {} for {} '.format(self.plm_name,
              self.sampling_type, self.relation_type) + '==' * 4)
        self.out_dir = check_dir('re_datasets/{}/{}+{}/'.format( \
                       self.relation_type, self.plm_name, self.sampling_type))        
        self.tokenizer = AutoTokenizer. \
            from_pretrained(self.cache_dir + self.plm_name)
        self.tokenizer.add_tokens(['@gm$', '@/gm$', '@di$', '@/di$',
                                   '@sm$', '@/sm$'])
        self.model = AutoModelForSequenceClassification. \
            from_pretrained(self.cache_dir + self.plm_name, 
                        num_labels = 3 if self.relation_type == 'GM2DI' else 5)
        self.model.resize_token_embeddings(len(self.tokenizer))
        self.max_length = self.model.config.max_position_embeddings
        self.device = torch.device('cuda:' + self.gpu_number
                                   if torch.cuda.is_available() else 'cpu')
        self.model.to(self.device)
        

    def _train(self):
        """
        Training process of pretrained language models, evaluate for dev 
        dataset after each epoch.
        Evaluation metric: F1-Score (macro).
        Inputs:
            out_dir/(train, dev)_input.data
        Outputs:
            out_dir/train_result.json
            out_dir/model.ckpt
        """
                
        self.process_re_input()
        
        self.optimizer = AdamW(self.model.parameters(), self.learning_rate)
        n_step = (self.n_train // self.batch_size + 1) * self.max_epoch
        self.scheduler = get_linear_schedule_with_warmup(self.optimizer,
                                                         n_step // 20, n_step)
        self.critertion = nn.CrossEntropyLoss()
        
        dev_batches = self.split_batches('dev')
        p_result = self.out_dir + 'train_result.json'
        print('\n>>  Training process.')
        print('  *learning rate    : {}'.format(self.learning_rate))
        print('  *batch size       : {}'.format(self.batch_size))
        print('  *max epoch        : {}'.format(self.max_epoch))
        print('  *earlystop epochs : {}\n'.format(self.earlystop_epochs))
        print('  EPOCH TRAIN-LOSS DEV-ACC DEV-F1 TIME(min)')
        tmp_f1, F1 = [], []
        t0 = time.time()
        for ep in range(self.max_epoch):
            Loss = []
            for inputs, labels in self.split_batches('train'):
                self.optimizer.zero_grad()
                logits = self.model(**inputs)['logits']
                loss = self.critertion(logits, labels)
                loss.backward()
                self.optimizer.step()
                self.scheduler.step()
                Loss.append(loss.item())         
            f1, acc, cm = self.cal_metric(dev_batches)
            print('  {:^5} {:^10.4f}  {:^6.4f} {:^6.4f} {:^9.2f}'. \
                  format(ep + 1, np.mean(Loss), acc, f1,
                         (time.time() - t0) / 60), end = '')
            
            if ep == 0 or f1 > F1[-1]:
                print(' *')
                if len(tmp_f1) > 0:
                    F1.extend(tmp_f1)
                    tmp_f1 = []
                F1.append(f1)
                pre_f1 = myjson(p_result)['f1'] if exists(p_result) else 0.0
                if f1 > pre_f1:
                    result = {'args': self.args, 'epoch': len(F1),
                              'f1': f1, 'acc': acc, 'cm': cm}
                    myjson(p_result, result)
                    torch.save(self.model.state_dict(),
                               self.out_dir + 'model.ckpt')        
            else:
                print('')
                if len(tmp_f1) == self.earlystop_epochs:
                    break
                else:
                    tmp_f1.append(f1)
                            
    
    def _evaluate(self):
        """
        Evaluate process for test annotations.
        Inputs:
            re_datasets/GM2(DI, SM)/test.tsv
        Outputs:
            out_dir/pre_result.tsv
        """
        
        print('\n>>  Evaluation process.')
    
        checkpoint = torch.load(self.out_dir + 'model.ckpt')
        self.model.load_state_dict(checkpoint)
        
        df = pd.read_csv('re_datasets/{}/test.tsv'. \
                     format(self.relation_type), sep = '\t')
        paras = list(df['paragraph'])
        labels, pres = list(df['label']), []
        for para in paras:
            _input = self.tokenizer(para, truncation = True,
                                    padding = 'max_length',
                                    max_length = self.max_length,
                                    return_tensors = 'pt')
            logits = self.cal_logits([_input]) 
            pres.append(np.argmax(logits, 1)[0])
        
        f1 = round(sm.f1_score(labels, pres, average = 'macro'), 4)
        acc = round(np.mean(np.array(labels) == np.array(pres)), 4)
        
        print('  --  Done. (F1: {:.4f}, ACC: {:.4f})'.format(f1, acc))
    
    
    def _predict(self):
        """
        Predict process for relation extraction.
        Inputs:
            ../pubtator3_gm/re_input_gm2(di, sm)_(0, 1).json
        Outputs:
            out_dir/re_result.json
        """
        
        print('\n>>  Prediction process.')
        
        p_result = self.out_dir + 're_result.json'
        if exists(p_result):
            print('  >>  exists.')
        else:        
            checkpoint = torch.load(self.out_dir + 'model.ckpt')
            self.model.load_state_dict(checkpoint)
            
            inputs, pairs, pmids, logits = [], [], [], []
            t0 = time.time()
            for k in range(2):
                re_input = myjson('../pubtator3_gm/re_input_{}_{}'. \
                                  format(self.relation_type.lower(), k))
                pair_keys = sorted(re_input.keys())
                n_pair = len(pair_keys)
                for i in range(n_pair):
                    pair = pair_keys[i]
                    for y in re_input[pair]:
                        pmid, para = y.split('|SEP|')
                        _input = self.tokenizer(para, truncation = True,
                                                padding = 'max_length',
                                                max_length = self.max_length,
                                                return_tensors = 'pt')
                        inputs.append(_input)
                        pairs.append(pair)
                        pmids.append(pmid)
                        if len(inputs) == self.batch_size:                    
                            logits.extend(self.cal_logits(inputs))
                            inputs = []              
                    if (i + 1) % 100 == 0 or (i + 1) == n_pair:
                        print('  --  {}_[{:5} | {:5}] ({:6}) ({:7.2f}mins)'. \
                              format(k, i + 1, n_pair, len(pairs), 
                                     (time.time() - t0) / 60))
            if len(inputs) > 0:
                logits.extend(self.cal_logits(inputs))

            result = {}
            for logit, pair, pmid in zip(logits, pairs, pmids):
                result[pair] = result.get(pair, []) + [[pmid, logit]]
            myjson(p_result, result)


    def process_re_input(self):
        """
        Process re inputs (PubTator3 paragraphs) by tokenizer.
        Inputs:
            re_datasets/relation_type/(train, dev).tsv
        Outputs:
            out_dir/(train, dev)_input.data
        """
        
        print('\n            ALL Unrelated ', end = '')
        if self.relation_type == 'GM2DI':
            print('Activate Inhibit')
        else:
            print('Produce  Consume Sensitive-to Resistant-to')            
            
        for key in ['train', 'dev']:
            p_input = '{}{}_input.data'.format(self.out_dir, key)        
            if not exists(p_input):
                inputs = []                
                df = pd.read_csv('re_datasets/{}/{}.tsv'. \
                                 format(self.relation_type, key), sep = '\t')
                for para, label in zip(df['paragraph'], df['label']):
                    _input = self.tokenizer(para, truncation = True,
                                            padding = 'max_length',
                                            max_length = self.max_length,
                                            return_tensors = 'pt')
                    inputs.append([_input, label])
                mypickle(p_input, inputs)
            else:
                inputs = mypickle(p_input)
            
            exec('self.{} = inputs'.format(key))
            exec('self.n_{} = len(inputs)'.format(key))
            labels = [_[1] for _ in inputs]
            counts = [labels.count(x) for x in range(5)]
            print('  #{:5} : {:4}   {:4}      {:4}    {:4}'.format(key,
                  len(inputs), counts[0], counts[1], counts[2]), end = '')
            if self.relation_type == 'GM2SM':
                print('      {:4}         {:4}'.format(counts[3], counts[4]))
            else:
                print('')


    def split_batches(self, key):
        """Split and generate input batches."""
        
        data = eval('self.' + key + '.copy()')
        if key == 'train':
            if self.sampling_type != 'none':
                data = self.get_banlance_samples(data)
            random.shuffle(data)
        bs = self.batch_size
        n_batch = len(data) // bs
        idxes = [data[i * bs: (i + 1) * bs] for i in range(n_batch)]
        if len(data) % bs != 0:
            idxes.append(data[n_batch * bs: ])
        batches = []
        for idx in idxes:
            inputs = {}
            for x in ['input_ids', 'attention_mask']:
                y = torch.vstack([_[0][x] for _ in idx]).to(self.device)
                inputs[x] = y
            label = torch.tensor([_[1] for _ in idx]).to(self.device)
            batches.append((inputs, label))
        
        return batches
    
        
    def get_banlance_samples(self, data):
        """
        (1) sub: based on the minority category number, subsampling the
                 majority and middle category.
        (2) sub-up: based on the middle category number, subsampling the 
                    majority category and upsampling the minority category.
        (3) up: based on the majority category number, upsampling the
                minority and middle category.
        """
        
        n_label = 3 if self.relation_type == 'GM2DI' else 5
        split_datas = [[] for i in range(n_label)]
        for _ in data:
            split_datas[_[1]].append(_)
        counts = sorted([len(split_data) for split_data in split_datas])
        if self.sampling_type == 'sub':
            target = counts[0]
        elif self.sampling_type == 'sub-up':
            target = counts[n_label // 2]
        elif self.sampling_type == 'up':
            target = counts[-1]
        data = []
        for split_data in split_datas:
            n = len(split_data)
            data.extend(split_data * (target // n))
            data.extend(random.sample(split_data, target % n))
                
        return data
    
    
    def cal_metric(self, batches):
        """
        Calculate classification f1 score (macro), accuracy (acc) 
        and comfusion matrix (cm).
        """
        
        Pre, Label = np.array([]), np.array([])
        with torch.no_grad():
            for inputs, labels in batches:
                logits = self.model(**inputs)['logits']
                _, pre = torch.max(logits.data, 1)
                Pre = np.hstack((Pre, pre.cpu().data.numpy()))
                Label = np.hstack((Label, labels.cpu().data.numpy()))
        f1 = round(sm.f1_score(Label, Pre, average = 'macro'), 4)
        acc = round(np.mean(Label == Pre), 4)
        cm = sm.confusion_matrix(Label, Pre).tolist()
                
        return f1, acc, cm


    def cal_logits(self, inputs):
        """Calculate logits for all classes."""
        
        batch = {}
        for x in ['input_ids', 'attention_mask']:
            y = torch.vstack([_[x] for _ in inputs]).to(self.device)
            batch[x] = y
        
        with torch.no_grad():
            logits = self.model(**batch)['logits'].cpu().data.numpy()
        logits = np.exp(logits) / np.sum(np.exp(logits), axis = 1,
                                         keepdims = True)
        logits = [[round(x, 4) for x in logit] for logit in logits.tolist()]
        
        return logits
    
    
    def get_subword(self, paras):
        """Get subword result for paragraphs."""
        
        dic = self.tokenizer.vocab
        rev_dic = {y: x for x, y in dic.items()}
        
        for para in paras:
            print('Raw sentence: ' + para)
            print('Subword result: ', end = '')
            inputs = self.tokenizer(para, truncation = True,
                                    padding = 'max_length',
                                    max_length = self.max_length,
                                    return_tensors = 'pt')
            for x in inputs.input_ids.numpy()[0]:
                if x != 0:
                    print(rev_dic[x], end = ' ')
            print('\n')
    
    
    def run(self):
        """Runing Process"""
        
        if self.do_train:
            self._train()
        if self.do_evaluate:
            self._evaluate()            
        if self.do_predict:
            self._predict()