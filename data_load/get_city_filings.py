'''
To combine all the City Filings into one csv file
example usage:
python3 get_city_filings.py "../../../../../../WhoPays/NYCCFB_Data_02_21_2021" "2001_City_contribution.csv,2003_City_contribution.csv,2005_City_contribution.csv,2009_City_contribution.csv,2013_City_contribution.csv,2017_City_Contributions.csv,2021_City_Contributions.csv" "city_02212021_processed.csv"
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
        df = pd.read_csv(filings_dir+"/"+file,encoding="latin1", error_bad_lines=False)
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
    print(main_df.columns.values)
    main_df.to_csv(filings_dir+"/"+outfile_name, index=False)
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