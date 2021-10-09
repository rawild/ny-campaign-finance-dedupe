import dj_database_url
import psycopg2
import psycopg2.extras
import argparse, os
import pandas as pd


def upload_latlng(donors_latlng_file):
    print('getting csv for processing...')
    df = pd.read_csv(donors_latlng_file, encoding='latin-1')
    df.to_csv('donors_latlng_for_upload.csv', index=False)


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


    print('creating temp table...')
    c.execute("CREATE TEMP TABLE tmp_d"
             "(cluster_id INTEGER, lat DOUBLE PRECISION, "
             "lng DOUBLE PRECISION) ")
    conn.commit()

    with open('donors_latlng_for_upload.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(cluster_id, lat, lng) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    print('updating processed_donors...')
    c.execute("UPDATE processed_donors as p"
            " SET lat = t.lat, "
            " lng = t.lng "
            " FROM tmp_d as t"
            " WHERE p.cluster_id = t.cluster_id ")
    conn.commit()
    
    c.close()
    conn.close()

    os.remove('donors_latlng_for_upload.csv')

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='upload_latlng_info')
    parser.add_argument('donors_latlng_file', help='file with donors latlng info')
    args=parser.parse_args()
    upload_latlng(args.donors_latlng_file)
    finish()