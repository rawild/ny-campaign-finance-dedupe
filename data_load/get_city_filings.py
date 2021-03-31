'''
To combine all the City Filings into one csv file
example usage:
python3 get_city_filings.py "../../NYCCFB_Data_02_21_2021" "2001_City_contribution.csv,2003_City_contribution.csv,2005_City_contribution.csv,2009_City_contribution.csv,2013_City_contribution.csv,2017_City_Contributions.csv,2021_City_Contributions.csv" "city_02212021_processed.csv"
'''

import argparse, sys
import pandas as pd

def get_middle(name):
    if (len(str(name).split(", "))>1) and (len(str(name).split(", ")[1].split(" "))>1):
        return str(name).split(", ")[1].split(" ")[1]
    else:
        return ""

def get_first(name):
    if len(str(name).split(", "))>1:
        return str(name).split(", ")[1].split(" ")[0]
    else:
        return name

def compile_filings_city(filings_dir,infile_names, outfile_name):
    for i,file in enumerate(infile_names):
        year = file.split("_")[0]

        df = pd.read_csv(filings_dir+"/"+file,encoding="latin1", dtype={0:"int",1:"str",2:"str",3:"str",4:"str",5:"str",6:"str",7:"str",8:"str",9:"str",
            10:"str",11:"str",12:"str",13:"str",14:"str",15:"str",16:"str",17:"str",18:"str",19:"str",
            20:"str",21:"str",22:"str",23:"str",24:"str",25:"str",26:"str",27:"str",28:"str",
            29:"str",30:"str",31:"str",32:"str",33:"str",34:"str",35:"str",36:"str",37:"str",38:"str",39:"str",
            40:"str",41:"str",42:"str",43:"str",44:"str",45:"str",46:"str",47:"str",48:"str",49:"str",50:"str",51:"str",52:"str"}, error_bad_lines=False)
        if int(year) < 2009:
            df['RECIPID'] = df['CANDID']
        if int(year) > 2005:    
            df['CANDLAST'] = df['RECIPNAME'].apply(lambda x: x.split(",")[0])
            df['CANDFIRST'] = df['RECIPNAME'].apply(lambda x: x.split(", ")[1].split(" ")[0] if len(x.split(","))>1 else "")
            df['CANDMI'] = df['RECIPNAME'].apply(lambda x: get_middle(x) if len(x.split(", "))>1 and len(x.split(", ")[0].split(" "))>1 else "")
        df['DONORCORP'] = df.apply(lambda row: row['NAME'] if row["C_CODE"] != "IND" else "", axis=1)
        df['DONORLAST'] = df.apply(lambda row: str(row['NAME']).split(",")[0] if row["C_CODE"] == "IND" else "", axis=1)
        df['DONORFIRST'] = df.apply(lambda row: get_first(row['NAME']) if row["C_CODE"] == "IND" else "", axis=1)
        df['DONORMI'] = df.apply(lambda row: get_middle(row['NAME']) if row["C_CODE"] == "IND" else "", axis=1)

        if i == 0:
            columns = ["ELECTION","OFFICECD","RECIPID","CANCLASS","CANDFIRST",
                "CANDLAST","CANDMI",
                "COMMITTEE","FILING","SCHEDULE","PAGENO","SEQUENCENO",
                "REFNO","DATE","REFUNDDATE","NAME","C_CODE","DONORCORP","DONORFIRST",
                "DONORLAST", "DONORMI",
                "STRNO","STRNAME","APARTMENT","BOROUGHCD","CITY",
                "STATE","ZIP","OCCUPATION","EMPNAME","EMPSTRNO",
                "EMPSTRNAME","EMPCITY","EMPSTATE","AMNT","MATCHAMNT",
                "PREVAMNT","PAY_METHOD","INTERMNO","INTERMNAME","INTSTRNO",
                "INTSTRNM","INTAPTNO","INTCITY","INTST","INTZIP",
                "INTEMPNAME","INTEMPSTNO","INTEMPSTNM","INTEMPCITY","INTEMPST",
                "INTOCCUPA","PURPOSECD","EXEMPTCD","ADJTYPECD","RR_IND","SEG_IND",
                "INT_C_CODE"]
            main_df = pd.DataFrame(None, columns=columns)
        main_df = pd.concat([main_df,df], ignore_index=True, sort=False)
    main_df = main_df[columns]
    main_df['DATE'] = main_df['DATE'].str.extract(r'(\d{1,2}\/\d{1,2}\/\d{4})')
    main_df['REFUNDDATE'] = main_df['REFUNDDATE'].str.extract(r'(\d{1,2}\/\d{1,2}\/\d{4})')
    print(main_df.columns.values)
    main_df.to_csv(filings_dir+"/"+outfile_name, index=False)
    print('final length: '+str(main_df.shape[0]))

def finish():
    print("Finished")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='fix all reports')
    parser.add_argument('filings_dir', help='directory that contains the city cfb files')
    parser.add_argument('infile_names', help='comma seperated list of infilenames')
    parser.add_argument('outfile_name', help = 'desired output name')
    args=parser.parse_args()
    compile_filings_city(str(args.filings_dir),args.infile_names.split(","), str(args.outfile_name))
    finish()