from data_load.get_samples import (get_filers, get_filings)
from data_load.init_postgres_db import processFiles
from data_load.clean_donors import address_cleaning
from dedupe_extension.campaign_finance_dedupe import run_dedupe
import time
import argparse


def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='run dedupe test')
    parser.add_argument('--type', '-t', help='CORP or IND')
    args = parser.parse_args()
    if args.type:
        type = args.type 
    else: 
        type = 'CORP'
    round_data = [[.02,3],[.04,5],[.06,7],[.08,13],[.1,17]]
    #round_data = [[.02,3]]
    for n in [3,4]: 
        #print(f'starting round {i} at {time.strftime("%H:%M",time.localtime())}...')
        #print('get samples..')
        #train_proportion = 1 - round[0]
        #filers = get_filers('../commcand', train_proportion, round[1])
        #filers_filename = '../sample_filers_'+str(i)+'.csv'
        #filers.to_csv(filers_filename, index=False)
        #filings = get_filings('data_load', train_proportion, round[1])
        #filings_filename = '../sample_filings_'+str(i)+'.csv'
        #filings.to_csv(filings_filename, index=False)
        #print('load into db')
        #processFiles(filers_filename, filings_filename)
        #address_cleaning()
        if type == 'CORP':
            settings_filename = 'dedupe_extension/settings/settings_CORP_ext_'+str(n)
        else:
            settings_filename = 'dedupe_extension/settings/settings_IND_1_comb'
        print(f'using {settings_filename}...')
        training_file = 'training_'+time.strftime('%d_%m_%y_%H%M', time.localtime())+'.json' 
        run_dedupe(settings_filename,training_file,type)
    finish()