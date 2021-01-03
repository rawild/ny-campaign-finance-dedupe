import argparse
import re
from all_txt_to_csv import (get_filers,get_filings)
from init_postgres_db import (processFiles)
import pandas as pd

def fix_filings(filings_dir, infile_name, outfile_name):
    orig_filings = open(filings_dir+"/"+infile_name,'r', encoding="latin-1")
    fixed_filings = open(filings_dir+"/"+outfile_name,'w', encoding="latin-1")
    bad_filings = open(filings_dir+"/"+"bad_"+outfile_name,'w', encoding="latin-1")
    target_filings = open(filings_dir+"/"+"target_"+outfile_name,'w', encoding="latin-1")
    for i,line in enumerate(orig_filings):
        if len(line.split('","')) != 30:
            bad_filings.write(line)
        else:
            line1=line
            line=line.replace('LABORERS"",','LABORERS",')
            line=line.replace(',",',',"",')
            line=line.replace('"O"C','"O\'')
            line=line.replace('"O"C','"O\'')
            line=line.replace(',"          "",',',"",')
            line=line.replace(',"",ICHAEL",',',"ICHAEL",')
            line=line.replace('"", JR."','"JR."')
            line=re.sub('"{2}(?=\w)','"',line)
            line=re.sub('(?<=[\w .,]),"(?!,")','',line)
            line=re.sub('(?<!,)""",','",',line)
            line=re.sub(',""",',',"",',line)
            line=re.sub('(?<!,)""','"',line)
            line=re.sub('"",(?=\w)','"","',line)
            line=re.sub('(?<=\w)\.,"","","","","(?=\w)','.","","","","',line)
            line=re.sub(',"NEW YORK","","NY","(?=\d)',',"NEW YORK","NY","',line)
            line=re.sub(',"PITTSBURGH","","PA","(?=\d)',',"PITTSBURGH","PA","',line)
            line=re.sub('(?<=\w)","","LAKE PLACID","','","LAKE PLACID","',line)
            line=re.sub('(?<=\w)","","GARDEN CITY","','","GARDEN CITY","',line)
            line=re.sub('(?<=\w)","","ALBANY","','","ALBANY","',line)
            line=re.sub('(?<=\w)","","LONG BEACH","','","LONG BEACH","',line)
            line=re.sub('(?<=\w)","","COLUMBUS","','","COLUMBUS","',line)
            line=re.sub('(?<=\w),15D","","","","","2\w',',15D","","","","2',line)
            line=line.replace('PO BOX 113","","POINT LOOKOUT",','PO BOX 113","POINT LOOKOUT",')
            line=line.replace('SPELLMAN","","659 ELY','SPELLMAN","659 ELY')
            line=line.replace('OFFICERS PAC INC","","","","","504 EAST','OFFICERS PAC INC","","","","504 EAST')
            line=line.replace('GRECO & SLISZ","","FRANCIS','GRECO & SLISZ","FRANCIS')
            line=line.replace('";,""','"",""')
            line=line.replace(',"Z;,"",',',"",')
            line=line.replace('", RONNIE E"','"RONNIE E"')
            line=line.replace(',"",, 41 MAPLEWOOD ST"',',"41 MAPLEWOOD ST"')
            line=line.replace('"MARY","E","HALL","", APT. C-19"','"MARY","E","HALL","APT. C-19"')
            line=line.replace(',"","", MESSENGER, & PEARL ASSOC."',',"","MESSENGER, & PEARL ASSOC."')
            line=line.replace('"01/23/2006","","","","",","","","","","","","","","","0","","","","","","DUPLICATE FILING FROM OFF CYCLE 2006"','"01/23/2006","","","","","","","","","","","","","","","0","","","","","","DUPLICATE FILING FROM OFF CYCLE 2006"')
            line=line.replace('"C38478","K","A","2010","5637","04/23/2009","","CAN","","","",","",","",","",","",","",","","",","","0","","","","","","TO DELETE DUPLICATE ENTRY","","","JR","03/25/2011 13:51:20"',
            '"C38478","K","A","2010","5637","04/23/2009","","CAN","","","","","","","","","","","","0","","","","","","TO DELETE DUPLICATE ENTRY","","","JR","03/25/2011 13:51:20"')
            line=line.replace('"2285 "PEACHTREE ROAD, NE, UNIT 405","ATLANTA"','"2285 PEACHTREE ROAD, NE, UNIT 405","ATLANTA"')
            if len(line.split('","')) != 30:
                bad_filings.write(line1)
            else:
                fixed_filings.write(line)

    orig_filings.close()
    fixed_filings.close()
    bad_filings.close()
def finish():
    print("Finished")


def fix_testing(filings_dir, infile_name, outfile_name):
    fix_filings(filings_dir,infile_name, outfile_name)
    filers = get_filers('../../boe_data_12_30_20/commcand')
    filings= get_filings(filings_dir,outfile_name)
    filers.to_csv('filers_'+outfile_name.split('.')[0]+'.csv',index=False)
    filings.to_csv('filings_'+outfile_name.split('.')[0]+'.csv',index=False)
    processFiles('filers_'+outfile_name.split('.')[0]+'.csv','filings_'+outfile_name.split('.')[0]+'.csv')
    finish()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='fix all reports')
    parser.add_argument('filings_dir', help='route to filings files directory')
    parser.add_argument('infile_name', help='original filings name ')
    parser.add_argument('outfile_name', help = 'desired output name')
    args=parser.parse_args()
    fix_filings(str(args.filings_dir), str(args.infile_name),str(args.outfile_name))
    finish()