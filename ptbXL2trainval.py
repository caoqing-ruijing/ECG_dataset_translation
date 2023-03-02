import os,sys,shutil
import pandas as pd
import numpy as np
import wfdb
import ast
import json
from tqdm import tqdm
import gzip

def mkdir_without_del(path):
    if not os.path.exists(path):
        os.makedirs(path)

def mkdir_with_del(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)

def check_path(path):
    main_dir = os.path.dirname(path)
    mkdir_without_del(main_dir)

def process(input_dir,out_dir):
    csv_path = input_dir+'ptbxl_database.csv'

    Y = pd.read_csv(csv_path, index_col='ecg_id')
    filename_hr = Y['filename_hr'].tolist()

    # ['I', 'II', 'III', 'AVR', 'AVL', 'AVF', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6']

    for filename_i in tqdm(filename_hr):

        out_name = filename_i+'.json'

        file_path = input_dir+filename_i
        data_raw = wfdb.rdsamp(file_path)

        data_json = {}
        lead_names = data_raw[1]['sig_name']        
        for idx,lead_name in enumerate(lead_names):
            data_i = data_raw[0][:,idx].tolist()

            lead_name = lead_name.replace('A','a')
            data_json[lead_name] = data_i

        out_dir_path = out_dir+out_name
        check_path(out_dir_path)

        # with open(out_dir_path, 'w') as fw:
        #     json.dump(data_json,fw)
        with gzip.open(out_dir_path, 'w') as fout:
            fout.write(json.dumps(data_json).encode('utf-8'))


def clean_json(args):
    file_path = args[0]
    out_dir_path = args[1]
    if os.path.exists(out_dir_path):
        pass
    else:
        data_raw = wfdb.rdsamp(file_path)
        data_json = {}
        lead_names = data_raw[1]['sig_name']        
        for idx,lead_name in enumerate(lead_names):
            data_i = data_raw[0][:,idx].tolist()

            lead_name = lead_name.replace('A','a')
            data_json[lead_name] = data_i

        # out_dir_path = out_dir+out_name
        check_path(out_dir_path)

        with gzip.open(out_dir_path, 'w') as fout:
            fout.write(json.dumps(data_json).encode('utf-8'))



def process(input_csv,name_csv,column_name,input_dir,out_dir):

    df_stats = pd.read_csv(name_csv)
    label_name = df_stats['names'].tolist()

    Y = pd.read_csv(input_csv, index_col='ecg_id')
    # filename_hr = Y['filename_hr'].tolist()
    # diagnostic_superclass = Y[column_name].tolist()
    # diagnostic_superclass = [ast.literal_eval(i) for i in diagnostic_superclass]

    unqie_cls_dict = {i:[0,0,0] for i in label_name}

    test_fold = 10
    y_train_df = Y[(Y.strat_fold != test_fold)]
    y_test_df = Y[Y.strat_fold == test_fold]

    # for train
    filename_hr_train = y_train_df['filename_hr'].tolist()
    diagnostic_superclass_train = y_train_df[column_name].tolist()
    diagnostic_superclass_train = [ast.literal_eval(i) for i in diagnostic_superclass_train]

    train_paths,train_labels = [],[]
    for i in range(len(filename_hr_train)):
        filename_i = filename_hr_train[i]
        diagnostic_superclass_i = diagnostic_superclass_train[i]

        out_name = filename_i+'.json'
        file_path = input_dir+out_name
        if not os.path.exists(file_path):
            assert 1>2,file_path
        
        if len(diagnostic_superclass_i) > 0:
            valid_label_i =[]
            for label_i in diagnostic_superclass_i:
                if label_i in unqie_cls_dict:
                    unqie_cls_dict[label_i][0]+=1
                    unqie_cls_dict[label_i][1]+=1
                    valid_label_i.append(label_i)
            
            train_paths.append(filename_i)
            train_labels.append(valid_label_i)

    # for test
    filename_hr_val = y_test_df['filename_hr'].tolist()
    diagnostic_superclass_val = y_test_df[column_name].tolist()
    diagnostic_superclass_val = [ast.literal_eval(i) for i in diagnostic_superclass_val]

    val_paths,val_labels = [],[]
    for i in range(len(filename_hr_val)):
        filename_i = filename_hr_val[i]
        diagnostic_superclass_i = diagnostic_superclass_val[i]

        out_name = filename_i+'.json'
        file_path = input_dir+out_name
        if not os.path.exists(file_path):
            assert 1>2,file_path
        
        if len(diagnostic_superclass_i) > 0:
            valid_label_i=[]
            for label_i in diagnostic_superclass_i:
                if label_i in unqie_cls_dict:
                    unqie_cls_dict[label_i][0]+=1
                    unqie_cls_dict[label_i][2]+=1
                    valid_label_i.append(label_i)
            
            val_paths.append(filename_i)
            val_labels.append(valid_label_i)


    inter_c = list(set(train_paths).intersection(val_paths))  
    if len(inter_c) >0:
        print('train and val intersetion',inter_c)
        assert 1>2


    # df = pd.DataFrame.from_dict(unqie_cls_dict)
    df_stats['all']=[unqie_cls_dict[i][0] for i in label_name]
    df_stats['train']=[unqie_cls_dict[i][1] for i in label_name]
    df_stats['val']=[unqie_cls_dict[i][2] for i in label_name]
    print(df_stats)
    df_stats.to_csv(out_dir+'k{}_train_val_stats.csv'.format(test_fold),index=False,encoding='utf-8_sig')

    df = pd.DataFrame()
    df['paths']=train_paths
    df['label_names']=train_labels
    df.to_csv(out_dir+'k{}_train.csv'.format(test_fold),index=False,encoding='utf-8_sig')

    df = pd.DataFrame()
    df['paths']=val_paths
    df['label_names']=val_labels
    df.to_csv(out_dir+'k{}_val.csv'.format(test_fold),index=False,encoding='utf-8_sig')

    print('train {} val {} all_valid {} all_orginal {}'.format(len(train_paths),len(val_paths),\
                            len(train_paths)+len(val_paths),len(Y)))
    print('all done')


if __name__ == "__main__":
    import multiprocessing as mp

    input_csv = './ptbxl_data.csv'
    data_dir = '/Users/wenjing_qiaoran/Downloads/计算机长尾方向paper/代码clone/data/raw_data/PTB_XL/clean_data/'


    # column_name = '0diag_main5'
    # column_name = '0diag_super23'
    column_name = '0diag_sub44'
    # column_name = '1form_19cls'
    # column_name = '2rhythm_12cls'
    name_csv =  './name_csv/{}.csv'.format(column_name)

    out_dir = data_dir+column_name+'/'


    mkdir_without_del(out_dir)
    print(column_name)
    process(input_csv,name_csv,column_name,data_dir,out_dir)



