'''
script to get a random sample from the filer and filings files from the nyboe massive files.
sample usage:
python get_samples.py '../../commcand' '.' .01 19
'''


import argparse
import pandas as pd
import time
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import StratifiedShuffleSplit


def get_filers(filers_dir, test_size, random_num):
    filers_columns_table = pd.read_table(filers_dir + '/FILEREC.txt',skiprows=7, nrows=14, sep=r'\s+', engine='python')
    filers_columns_table.head()
    filers_columns_names=filers_columns_table["FIELD"]
    filers_columns_list=list(filers_columns_names.values)
    filers_df = pd.read_csv(filers_dir + "/COMMCAND.txt", header=None, names=filers_columns_list, encoding="unicode_escape")
    filers_df['COMMITTEE_TYPE'].fillna(value=0,inplace=True)
    filers_df_new = filers_df[~filers_df['COMMITTEE_TYPE'].isin(['7HV','INACTIVE'])]
    encoder = LabelEncoder()
    filers_committee_type = filers_df_new['COMMITTEE_TYPE'].astype(str)
    filers_committee_type_encoded = encoder.fit_transform(filers_committee_type)
    filers_df_new.loc[:,'COMMITTEE_TYPE_NUM'] = filers_committee_type_encoded
    filers_df_final =filers_df_new[~filers_df_new['COMMITTEE_TYPE_NUM'].isna()]
    filers_df_final = filers_df_final.astype({'OFFICE':'Int64','DISTRICT':'Int64'},errors='ignore')
    split_filers = StratifiedShuffleSplit(n_splits=1, test_size=test_size, random_state=int(random_num))
    for train_index, test_index in split_filers.split(filers_df_final, filers_df_final['COMMITTEE_TYPE_NUM']):
        filers_train = filers_df_final.iloc[train_index,:]
        filers_train.drop(columns='COMMITTEE_TYPE_NUM', inplace=True)
    return filers_train


def get_filings(filings_dir,test_size,random_num):
    columns = pd.read_table(filings_dir+'/EFSRECB.txt',skiprows=8, nrows=31, sep=r'\s+', engine='python')
    column_names = columns["FIELD"]
    column_list = list(column_names.values)
    transaction_types = ['A','B','C','D','E']
    try:
        df = pd.read_csv(filings_dir+"/ALL_REPORTS_fixed.txt",header=None, names=column_list, encoding="latin1")
        print(f"parsed ALL_REPORTS_fixed.txt")
        print(f"full filings: {df.shape}")
        df_inbound = df[df['TRANSACTION_CODE'].isin(transaction_types)]
        print(f"filtered filings: {df_inbound.shape}")
        split_filings = StratifiedShuffleSplit(n_splits=1, test_size=test_size, random_state=int(random_num))
        for train_index, test_index in split_filings.split(df_inbound, df_inbound['TRANSACTION_CODE']):
            filings_train = df_inbound.iloc[train_index,:]
        print(f"sample filings: {filings_train.shape}")
        filings_train.loc[:,'E_YEAR'] = filings_train['E_YEAR'].astype(int)
        filings_train['T3_TRID'].fillna(value=0,inplace=True)
        filings_train.loc[:,'T3_TRID'] = filings_train['T3_TRID'].astype(int)
        filings_train.loc[:,'CONTRIB_TYPE_CODE_25']=filings_train['CONTRIB_TYPE_CODE_25'].apply(lambda x:  x if pd.isna(x) else str(x)[0:1])
        filings_train.loc[:,'CHECK_NO_60']=filings_train['CHECK_NO_60'].apply(lambda x: str(x).strip())
    except:
        print(f"failed file: ALL_REPORTS")
    return filings_train

def finish():
    print("Finished")
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='get samples')
    parser.add_argument('filers_dir', help='route to filers files directory')
    parser.add_argument('filings_dir', help='route to filings files directory')
    parser.add_argument('sample_size', help='proportional size of sample (.25)')
    parser.add_argument('random_num', help='random seed for the split')
    args=parser.parse_args()
    test_size = 1-float(args.sample_size)
    filers = get_filers(str(args.filers_dir),test_size,args.random_num)
    filings = get_filings(str(args.filings_dir),test_size,args.random_num)
    filers.to_csv("sample_filers_"+time.strftime('%d_%m_%y_%H%M', time.localtime())+".csv", index=False)
    filings.to_csv("sample_filings_"+time.strftime('%d_%m_%y_%H%M', time.localtime())+".csv", index=False)
    finish()





