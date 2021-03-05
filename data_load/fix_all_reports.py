"""
Source Control: 
This whole script needs to be re-written in light of the updated data format From NYSBOE on 01/22/2021 
Old method: fix_filings
New method: compile_filings_state
Note that as of 02/14/2021 the file format from the state BOE was not accurately described in the
"FileFormatReference.pdf" they issued. The column CAND_COMM_NAME does not exist and there is a second
address column between FLNG_ENT_ADD1 and FLNG_ENT_CITY that is not described in the documentation.
Feels like they are actively trying to troll
sample call:
python3 fix_all_reports.py "../../../../../../WhoPays/NYSBOE_Data_02142021" "02142021_processed.csv"
"""
import argparse, sys
import pandas as pd

def compile_filings_state(filings_dir, outfile_name):
    data_locations = [{"directory": "ALL_REPORTS_CountyCandidate", "file": "COUNTY_CANDIDATE.csv"},
    {"directory":"ALL_REPORTS_CountyCommittee","file": "COUNTY_COMMITTEE.csv"},
    {"directory":"ALL_REPORTS_StateCandidate","file":"STATE_CANDIDATE.csv"},
    {"directory":"ALL_REPORTS_StateCommittee","file":"STATE_COMMITTEE.csv"}]
    columns = ["FILER_ID","FILER_PREVIOUS_ID","ELECTION_YEAR","ELECTION_TYPE",
        "COUNTY_DESC","FILING_ABBREV","FILING_DESC","R_AMEND","FILING_CAT_DESC",
        "FILING_SCHED_ABBREV","FILING_SCHED_DESC","LOAN_LIB_NUMBER","TRANS_NUMBER","TRANS_MAPPING",
        "SCHED_DATE","ORG_DATE","CNTRBR_TYPE_DESC","CNTRBN_TYPE_DESC","TRANSFER_TYPE_DESC",
        "RECEIPT_TYPE_DESC","RECEIPT_CODE_DESC","PURPOSE_CODE_DESC","R_SUBCONTRACTOR","FLNG_ENT_NAME",
        "FLNG_ENT_FIRST_NAME","FLNG_ENT_MIDDLE_NAME","FLNG_ENT_LAST_NAME","FLNG_ENT_ADD1",
        "FLNG_ENT_ADD2","FLNG_ENT_CITY",
        "FLNG_ENT_STATE","FLNG_ENT_ZIP","FLNG_ENT_COUNTRY","PAYMENT_TYPE_DESC","PAY_NUMBER",
        "OWED_AMT","ORG_AMT","LOAN_OTHER_DESC","TRANS_EXPLNTN","R_ITEMIZED",
        "R_LIABILITY","ELECTION_YEAR_2","OFFICE_DESC","DISTRICT","DIST_OFF_CAND_BAL_PROP"]
    main_df = pd.DataFrame(None,columns=columns)
    console_out = sys.stderr
    for file in data_locations:
        with open(filings_dir+"/"+file["directory"]+"/"+file["file"].split(".")[0]+"_badlines.txt","w") as f:
            sys.stderr=f
            df = pd.read_csv(filings_dir+"/"+file["directory"]+"/"+file["file"], header=None, encoding="latin1", error_bad_lines=False, 
            dtype={0:"int",1:"str",2:"str",3:"str",4:"str",5:"str",6:"str",7:"str",8:"str",9:"str",
            10:"str",11:"str",12:"str",13:"str",14:"str",15:"str",16:"str",17:"str",18:"str",19:"str",
            20:"str",21:"str",22:"str",23:"str",24:"str",25:"str",26:"str",27:"str",28:"str",
            29:"str",30:"str",31:"str",32:"str",33:"str",34:"str",35:"str",36:"str",37:"str",38:"str",39:"str",
            40:"str",41:"str",42:"str",43:"str",44:"str"})
            df.rename(columns={0:"FILER_ID",1:"FILER_PREVIOUS_ID",2:"ELECTION_YEAR",3:"ELECTION_TYPE",
            4:"COUNTY_DESC",5:"FILING_ABBREV",6:"FILING_DESC",7:"R_AMEND",8:"FILING_CAT_DESC",
            9:"FILING_SCHED_ABBREV",10:"FILING_SCHED_DESC",11:"LOAN_LIB_NUMBER",12:"TRANS_NUMBER",13:"TRANS_MAPPING",
            14:"SCHED_DATE",15:"ORG_DATE",16:"CNTRBR_TYPE_DESC",17:"CNTRBN_TYPE_DESC",18:"TRANSFER_TYPE_DESC",
            19:"RECEIPT_TYPE_DESC",20:"RECEIPT_CODE_DESC",21:"PURPOSE_CODE_DESC",22:"R_SUBCONTRACTOR",23:"FLNG_ENT_NAME",
            24:"FLNG_ENT_FIRST_NAME",25:"FLNG_ENT_MIDDLE_NAME",26:"FLNG_ENT_LAST_NAME",27:"FLNG_ENT_ADD1",
            28:"FLNG_ENT_ADD2",29:"FLNG_ENT_CITY",
            30:"FLNG_ENT_STATE",31:"FLNG_ENT_ZIP",32:"FLNG_ENT_COUNTRY",33:"PAYMENT_TYPE_DESC",34:"PAY_NUMBER",
            35:"OWED_AMT",36:"ORG_AMT",37:"LOAN_OTHER_DESC",38:"TRANS_EXPLNTN",39:"R_ITEMIZED",
            40:"R_LIABILITY",41:"ELECTION_YEAR_2",42:"OFFICE_DESC",43:"DISTRICT",44:"DIST_OFF_CAND_BAL_PROP"}, inplace=True)
            main_df = pd.concat([main_df,df], ignore_index=True, sort=False)
            sys.stderr.close()
    sys.stderr=console_out
    main_df.to_csv(filings_dir+"/"+outfile_name, index=False)
    main_df.head(200000).to_csv(filings_dir+"/"+outfile_name.split(".")[0]+"_cut.csv")
    print("final length: "+str(main_df.shape[0]))


def finish():
    print("Finished")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='fix all reports')
    parser.add_argument('filings_dir', help='route to filings files directory')
    parser.add_argument('outfile_name', help = 'desired output name')
    args=parser.parse_args()
    compile_filings_state(str(args.filings_dir), str(args.outfile_name))
    finish()