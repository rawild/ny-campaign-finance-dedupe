import argparse
import dj_database_url
import psycopg2
import psycopg2.extras

def generate_word_list(word):
    word_list = []
    word_list.append(word)
    word_consonants = word
    for vowel in ['a','e','i','o','u']:
        word_consonants = word_consonants.replace(vowel,'')
    for i in range(1,8):
        if i > 3 :
            word_list.append(word[0:i])
            word_list.append(word[0:i]+word_consonants[-2:]+word[-1])
        if i > 2: 
            word_list.append(word[0:i]+word_consonants[-1])
            word_list.append(word[0:i]+word_consonants[-2:])
            word_list.append(word[0]+word_consonants[0:i])
            word_list.append(word[0]+word_consonants[1:i])
        word_list.append(word[0:i]+word_consonants[-3:-1]+word[-1])
    for i in range(len(word)-1):
        switched = list(word)
        switched[i]=word[i+1]
        switched[i+1]=word[i]
        word_list.append("".join(switched))
    return word_list

def get_counts(word_list):
    db_conf = dj_database_url.config()
    read_con = psycopg2.connect(database=db_conf['NAME'],
                                user=db_conf['USER'],
                                password=db_conf['PASSWORD'],
                                host=db_conf['HOST'],
                                port=db_conf['PORT'],
                                cursor_factory=psycopg2.extras.RealDictCursor)
    results = []
    with read_con.cursor() as cur:
        for word in word_list:
            cur.execute(" SELECT count(*) as num FROM processed_donors "
                " WHERE name LIKE '% "+word+"%' ")
            for row in cur:
                results.append({ word:row["num"] })
    return results

def clean_donors():
    llc_regex = r"lim\w*\s*lia\w*\s*c\w*\s*"
    llp_regex = r"lim\w*\s*lia\w*\s*p\w*\s*"
    pac_regex = r"pol\w*\s*ac\w*\s*co\w*\s*"


def address_cleaning():
    address_words = [
        ["street","st", "str"],
        ["avenue", "ave"],
        ["drive", "dr"],
        ["road","rd"],
        ["suite", "ste"],
        ["lane", "ln"],
        ["boulevard", "blvd"],
        ["heights", "hgts"],
        ["highway", "hwy"],
        ["turnpike", "tpke"],
        ["terrace", "terr"],
        ["parkway", "pkwy"],
        ["place", "pl","plc"],
        ["court", "ct"],
        ["route", "rte"],
        ["circle", "cir"],
        ["plaza", "plz"],
        ["extension", "ext"],
        ["square","sq"],
        ["post office box","po box","p o box"]
    ]
    db_conf = dj_database_url.config()
    write_con = psycopg2.connect(database=db_conf['NAME'],
                                 user=db_conf['USER'],
                                 password=db_conf['PASSWORD'],
                                 host=db_conf['HOST'],
                                 port=db_conf['PORT'],)
    remove_periods(write_con)
    with write_con.cursor() as cur:
        for group in address_words:
            for i,word in enumerate(group):
                if (i) > 0:
                    cur.execute("UPDATE processed_donors "
                    "SET street = REPLACE(street, '"+ word +"', '"+ group[0] +"') "
                    "WHERE (street like '% "+ word +" %' or street like '% "+ word +"') AND street not like '%"+group[0]+"%'")

    write_con.commit()    
    name_cleaning(write_con)
        

def remove_periods(write_con):
    fields = ["street", "name"]
    with write_con.cursor() as cur:
        for field in fields:
            cur.execute("UPDATE processed_donors "
                            "SET "+ field +"= REPLACE("+ field +", '.', '') WHERE street != '.' ")
    write_con.commit()

def name_cleaning(write_con):
    abbreviation_sets = [["new york city","nyc"],["new york state"," nys","nys "," nys "],["new york"," ny"," ny "]]
    with write_con.cursor() as cur:
        for set in abbreviation_sets:
            for i, value in enumerate(set):
                if i > 0:
                    cur.execute("UPDATE processed_donors "
                            "SET name = REPLACE(name, '"+value+"',' "+set[0]+" ') "
                            "WHERE name like '%"+value+"%' and person != 1")
    write_con.commit()
                

if __name__ == '__main__':
    
    address_cleaning()
    print("Finished")