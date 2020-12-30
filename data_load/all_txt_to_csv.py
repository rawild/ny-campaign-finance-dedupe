'''
script to get a random sample from the filer and filings files from the nyboe massive files.
sample usage:
python all_txt_to_csv.py '../../commcand' '../../ALL_REPORTS' 'ALL_REPORTS_fixed.txt'
'''
import argparse
import pandas as pd
import time


def get_filers(filers_dir):
    filers_columns_table = pd.read_table(filers_dir + '/FILEREC.txt',skiprows=7, nrows=14, sep=r'\s+', engine='python')
    filers_columns_table.head()
    filers_columns_names=filers_columns_table["FIELD"]
    filers_columns_list=list(filers_columns_names.values)
    filers_df = pd.read_csv(filers_dir + "/COMMCAND.txt", header=None, names=filers_columns_list, encoding="unicode_escape")
    filers_df['COMMITTEE_TYPE'].fillna(value=0,inplace=True)
    filers_df_new = filers_df[~filers_df['COMMITTEE_TYPE'].isin(['7HV','INACTIVE'])]
    return filers_df_new


def get_filings(filings_dir, filings_file):
    columns = pd.read_table(filings_dir+'/EFSRECB.txt',skiprows=8, nrows=31, sep=r'\s+', engine='python')
    column_names = columns["FIELD"]
    column_list = list(column_names.values)
    transaction_types = ['A','B','C','D','E']
    try:
        df = pd.read_csv(filings_dir+"/"+filings_file,header=None, names=column_list, encoding="latin1")
        print(f"parsed {filings_file}")
        print(f"full filings: {df.shape}")
        df_inbound = df[df['TRANSACTION_CODE'].isin(transaction_types)]
        print(f"filtered filings: {df_inbound.shape}")
        df_inbound.loc[:,'E_YEAR'] = df_inbound['E_YEAR'].astype(int)
        df_inbound['T3_TRID'].fillna(value=0,inplace=True)
        df_inbound.loc[:,'T3_TRID'] = df_inbound['T3_TRID'].astype(int)
        df_inbound.loc[:,'CONTRIB_TYPE_CODE_25']=df_inbound['CONTRIB_TYPE_CODE_25'].apply(lambda x:  x if pd.isna(x) else str(x)[0:1])
        df_inbound.loc[:,'CHECK_NO_60']=df_inbound['CHECK_NO_60'].apply(lambda x: str(x).strip())
    except:
        print(f"failed file: {filings_file}")
    return df_inbound

def finish():
    print("Finished")
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='get samples')
    parser.add_argument('filers_dir', help='route to filers files directory')
    parser.add_argument('filings_dir', help='route to filings files directory')
    parser.add_argument('filings_file', help='proportional size of sample (.25)')
    args=parser.parse_args()
    filers = get_filers(str(args.filers_dir),)
    filings = get_filings(str(args.filings_dir),str(args.filings_file))
    filers.to_csv("sample_filers_"+time.strftime('%d_%m_%y_%H%M', time.localtime())+".csv", index=False)
    filings.to_csv("sample_filings_"+time.strftime('%d_%m_%y_%H%M', time.localtime())+".csv", index=False)
    finish()





