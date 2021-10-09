import dj_database_url
import psycopg2
import psycopg2.extras
import argparse, os
import pandas as pd


def upload_census_tract_info(census_tract_file, donors_tract_file):
    print('getting csv for processing...')
    df = pd.read_csv(donors_tract_file, encoding='latin-1')
    df.to_csv('donors_tract_for_upload.csv', index=False)


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
    c.execute("DROP TABLE IF EXISTS census_tracts")

    print('creating census tract table...')
    c.execute("CREATE TABLE census_tracts "
             "(sub_geo_id VARCHAR(100), "
               "geo_id VARCHAR(100), "
               "num_donors INTEGER, "
               "total_tract_donated DOUBLE PRECISION, "
               "avg_donated DOUBLE PRECISION, "
               "median_inc DOUBLE PRECISION, "
               "population INTEGER)")
    conn.commit()

    with open(census_tract_file, 'r+') as csv_file:
        c.copy_expert("COPY census_tracts "
                    "(sub_geo_id, geo_id, num_donors, " 
                    " total_tract_donated, avg_donated, "
                    " median_inc, population) "
                    "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
 
    print('adding columns to processed_donors...')
    c.execute("ALTER TABLE processed_donors "
               "ADD lat DOUBLE PRECISION, "
               "ADD lng DOUBLE PRECISION, "
               "ADD census_tract VARCHAR(100)"
               )
    conn.commit()
'''

    print('creating temp table...')
    c.execute("CREATE TEMP TABLE tmp_d"
             "(cluster_id INTEGER, lat DOUBLE PRECISION, "
             "lng DOUBLE PRECISION, census_tract VARCHAR(100)) ")
    conn.commit()

    with open('donors_tract_for_upload.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(cluster_id, lat, lng, census_tract) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    print('updating processed_donors...')
    c.execute("UPDATE processed_donors as p"
            " SET lat = t.lat, "
            " lng = t.lng, "
            " census_tract = t.census_tract "
            " FROM tmp_d as t"
            " WHERE p.cluster_id = t.cluster_id ")
    conn.commit()
    
    c.close()
    conn.close()

    os.remove('donors_tract_for_upload.csv')

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='upload_census_tract_info')
    parser.add_argument('census_tract_file', help='file with census tracts')
    parser.add_argument('donors_tract_file', help='file with donors tract info')
    args=parser.parse_args()
    upload_census_tract_info(args.census_tract_file,args.donors_tract_file)
    finish()