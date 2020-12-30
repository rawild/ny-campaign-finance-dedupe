import argparse
import re

def fix_filings(filings_dir, infile_name, outfile_name):
    orig_filings = open(filings_dir+"/"+infile_name,'r', encoding="latin-1")
    fixed_filings = open(filings_dir+"/"+outfile_name,'w', encoding="latin-1")
    for line in orig_filings:
        line=line.replace('LABORERS"",','LABORERS",')
        line=line.replace(',""",',',"",')
        line=line.replace(',",',',"",')
        line=line.replace('"O"C','"O\'')
        line=line.replace('"O"C','"O\'')
        line=line.replace(',"          "",',',"",')
        line=re.sub('"{2}\w','"',line)
        line=re.sub('\w"{2}','"',line)
        fixed_filings.write(line)

        #to do: above replace not catching all instances of """".. need to try other escapes?
        #to do: fix "O"C and "O"D
        #to do: regular expression for ,""char and char"", -> FIND DOG"", replace with  DOG", and ,""HAIR with ,"HAIR
        #to do: the ,"          "", -> ,"",
    orig_filings.close()
    fixed_filings.close()

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='fix all reports')
    parser.add_argument('filings_dir', help='route to filings files directory')
    parser.add_argument('infile_name', help='original filings name ')
    parser.add_argument('outfile_name', help = 'desired output name')
    args=parser.parse_args()
    fix_filings(str(args.filings_dir), str(args.infile_name),str(args.outfile_name))
    finish()