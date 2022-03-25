'''
Look up new cluster id - same UUID

Usage:
python3 get_new_cluster_ids.py "'postgresql://username:password@db-url:port/db?sslmode=require'" "old_cluster_id_file"

'''

import argparse, os
import dj_database_url
import psycopg2
import psycopg2.extras
import pandas as pd


def get_new_cluster_ids(second_db,cluster_id_file):
    
    db_conf = dj_database_url.config()
    db_conf_new=dj_database_url.parse(second_db)
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

    conn_new=psycopg2.connect(database=db_conf_new['NAME'],
                            user=db_conf_new['USER'],
                            password=db_conf_new['PASSWORD'],
                            host=db_conf_new['HOST'],
                            port=db_conf_new['PORT'])
    
    c_new = conn_new.cursor()
    
    print('creating temp cluster table...')
    c.execute("CREATE TEMP TABLE tmp_o"
             " (old_cluster_id INTEGER, uuid VARCHAR(200)) ")
    conn.commit()
    with open(cluster_id_file, 'r+') as csv_file:
        c.copy_expert("COPY tmp_o "
        "(old_cluster_id) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()

    print('Updating temp cluster_id table with uuid')
    c.execute("UPDATE tmp_o as o SET uuid = c.uuid "
        " FROM contributions as c, processed_donors as p "
        " WHERE p.donor_id = c.donor_id and o.old_cluster_id = p.cluster_id and p.cluster_id = p.donor_id")
    conn.commit()

    tmp_filename_1 = "temp_old_cluster_id_uuid.csv"
    with open(tmp_filename_1, 'w') as file_out:
        c.copy_expert("COPY (SELECT * FROM tmp_o) TO STDOUT WITH CSV HEADER",file_out)
    
    print('creating temp cluster_id table new dby...')
    c_new.execute("CREATE TEMP TABLE tmp_s"
             " (old_cluster_id INTEGER, uuid VARCHAR(200), new_cluster_id INTEGER) ")
    conn_new.commit()
    with open(tmp_filename_1, 'r+') as csv_file:
        c_new.copy_expert("COPY tmp_s "
        "(old_cluster_id, uuid) "
        "FROM STDIN CSV HEADER", csv_file)
    conn_new.commit()

    print('Updating temp cluster_id table with new_cluster_id')
    c_new.execute("UPDATE tmp_s as s SET new_cluster_id = p.cluster_id "
        " FROM contributions as c, processed_donors as p "
        " WHERE p.donor_id = c.donor_id and c.uuid = s.uuid")
    conn_new.commit()

    out_filename = "cluster_id_match.csv"
    with open(out_filename, 'w') as file_out:
        c_new.copy_expert("COPY (SELECT * FROM tmp_s) TO STDOUT WITH CSV HEADER",file_out)

    c.close()
    conn.close()

    c_new.close()
    conn_new.close()

    os.remove(tmp_filename_1)

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='carry_fwd_clusters')
    parser.add_argument('second_db', help='db connection info for newer records')
    parser.add_argument('cluster_id_file', help='file of old cluster ids to translate')
    args=parser.parse_args()
    get_new_cluster_ids(args.second_db, args.cluster_id_file)
    finish()