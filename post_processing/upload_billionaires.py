import dj_database_url
import psycopg2
import psycopg2.extras
import argparse, os
import pandas as pd


def upload_billionaire_ids(billionaire_file):
    print('getting csv for processing...')
    df = pd.read_csv(billionaire_file, dtype={0:'Int64',1:'Int64',2:'Int64',3:'str'}, encoding='latin-1')
    df = df[['cluster_id','billionaire', 'millionaire', 'note']]
    df.to_csv('billionaires_for_upload.csv', index=False)


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

    '''
    print('adding column to processed_donors...')
    c.execute("ALTER TABLE processed_donors "
               "ADD class VARCHAR(20), "
               "ADD note VARCHAR(200)")
    conn.commit()
    '''    
    print('creating temp table...')

    c.execute("CREATE TEMP TABLE tmp_d"
             "(cluster_id INTEGER, billionaire INTEGER, "
             "millionaire INTEGER, note VARCHAR(200)) ")
    conn.commit()

    with open('billionaires_for_upload.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(cluster_id, billionaire, millionaire, note) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    print('updating processed_donors...')
    c.execute("UPDATE processed_donors as p"
            " SET class = CASE WHEN t.billionaire = 1 THEN 'billionaire' WHEN t.millionaire = 1 THEN 'millionaire' ELSE NULL END, "
            " note = t.note "
            " FROM tmp_d as t"
            " WHERE p.cluster_id = t.cluster_id ")
    conn.commit()
    
    c.close()
    conn.close()

    os.remove('billionaires_for_upload.csv')

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='upload_billionaires')
    parser.add_argument('billionaire_file', help='file with billionaires and millionaires')
    args=parser.parse_args()
    upload_billionaire_ids(args.billionaire_file)
    finish()