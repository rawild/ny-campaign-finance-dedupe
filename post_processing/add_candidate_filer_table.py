import dj_database_url
import psycopg2
import psycopg2.extras
import argparse, os
import pandas as pd


def add_candidate_filer_mapping(candidate_filer_file):
   
    db_conf = dj_database_url.config()
    if not db_conf:
        raise Exception(
            'set DATABASE_URL environment variable with your connection, e.g. '
            'export DATABASE_URL=postgres://user:password@host/mydatabase'
        )

    conn = psycopg2.connect(database=db_conf['NAME'],
                            user=db_conf['USER'],
                            password=db_conf['PASSWORD'],
                            host=db_conf['HOST'],
                            port=db_conf['PORT'])

    c = conn.cursor()


   # print('creating table...')
    #c.execute("CREATE TABLE candidate_filer "
    #         "(candidate_id INTEGER, filer_id INTEGER)")
    #conn.commit()

    with open(candidate_filer_file, 'r+') as csv_file:
        c.copy_expert("COPY candidate_filer "
        "(candidate_id,filer_id) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    
    c.close()
    conn.close()



def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='file with candidates')
    parser.add_argument('candidate_filer_file', help='file candidates in it')
    args=parser.parse_args()
    add_candidate_filer_mapping(args.candidate_filer_file)
    finish()