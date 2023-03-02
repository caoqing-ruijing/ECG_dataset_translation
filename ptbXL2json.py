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

# def process(input_dir,out_dir):
#     csv_path = input_dir+'ptbxl_database.csv'

#     Y = pd.read_csv(csv_path, index_col='ecg_id')
#     filename_hr = Y['filename_hr'].tolist()

#     # ['I', 'II', 'III', 'AVR', 'AVL', 'AVF', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6']

#     for filename_i in tqdm(filename_hr):

#         out_name = filename_i+'.json'

#         file_path = input_dir+filename_i
#         data_raw = wfdb.rdsamp(file_path)

#         data_json = {}
#         lead_names = data_raw[1]['sig_name']        
#         for idx,lead_name in enumerate(lead_names):
#             data_i = data_raw[0][:,idx].tolist()

#             lead_name = lead_name.replace('A','a')
#             data_json[lead_name] = data_i

#         out_dir_path = out_dir+out_name
#         check_path(out_dir_path)

#         # with open(out_dir_path, 'w') as fw:
#         #     json.dump(data_json,fw)
#         with gzip.open(out_dir_path, 'w') as fout:
#             fout.write(json.dumps(data_json).encode('utf-8'))


def clean_json(args):
    file_path = args[0]
    out_dir_path = args[1]
    # if os.path.exists(out_dir_path):
    #     pass
    # else:
    data_raw = wfdb.rdsamp(file_path)
    data_json = {}
    lead_names = data_raw[1]['sig_name']        
    for idx,lead_name in enumerate(lead_names):
        data_i = data_raw[0][:,idx].tolist()

        lead_name = lead_name.replace('A','a')
        data_json[lead_name] = {'value':data_i}

    # out_dir_path = out_dir+out_name

    with gzip.open(out_dir_path, 'w') as fout:
        fout.write(json.dumps(data_json).encode('utf-8'))



def process(input_dir,out_dir):
    csv_path = input_dir+'ptbxl_database.csv'

    Y = pd.read_csv(csv_path, index_col='ecg_id')
    filename_hr = Y['filename_hr'].tolist()

    # ['I', 'II', 'III', 'AVR', 'AVL', 'AVF', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6']

    seg_infos_draw_args = []
    for filename_i in tqdm(filename_hr):
        out_name = filename_i+'.ecgzip'
        file_path = input_dir+filename_i
        out_dir_path = out_dir+out_name
        if not os.path.exists(out_dir_path):
            check_path(out_dir_path)
            seg_infos_draw_args.append([file_path,out_dir_path])

    pool = mp.Pool(15)
    result = pool.map(clean_json, seg_infos_draw_args)
    pool.close()
    print('all done')


if __name__ == "__main__":
    import multiprocessing as mp

    input_dir = '/home/raid_24T/qiaoran_data24T/Public_data/PTB_XL/ptb-xl-a-large-publicly-available-electrocardiography-dataset-1.0.1/'
    out_dir = './orginal_jsonzip_ecgzip/'

    mkdir_with_del(out_dir)
    process(input_dir,out_dir)



