## ECG dataset translation
This repository translate the public ECG dataset PTB-XL into a compress json format and train and validation set. For the purpose of [ECG EKnet](https://github.com/caoqing-ruijing/ECG_EKnet) training.


### Requirements
+ wfdb;
+ gzip;


### Usage
#### 1. trans the ECG file into json:
first set the path in `ptbXL2json.py`:
```
input_dir = './ptb-xl-a-large-publicly-available-electrocardiography-dataset-1.0.1/'
out_dir = './orginal_jsonzip_ecgzip/'
```

then run with:
`python ptbXL2json.py `


#### 2. split the PTB_XL into train val:


first set the path in `ptbXL2trainval.py`:

```
input_csv = './ptbxl_data.csv'
data_dir = './orginal_jsonzip_ecgzip/'
```

then run with:
`python ptbXL2trainval.py `

this will generate following csv for training:
`k0_train.csv`
`k0_val.csv`
`K0_train_val_stats.csv`

