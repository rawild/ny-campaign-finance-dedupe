'''
Carry fwd the human edit to the matching when new data is loaded - same UUID

Usage:
python3 carry_fwd_cleaning.py "export DATABASE_URL='postgresql://username:password@db-url:port/db?sslmode=require'"

'''

import argparse, os
import dj_database_url
import psycopg2
import psycopg2.extras
import pandas as pd


def carry_fwd_cluster(second_db):
    
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
    '''try:
        print('adding sector class and note columns')
        c_new.execute("ALTER TABLE processed_donors"
            " ADD COLUMN sector VARCHAR(50),"
            " ADD COLUMN note VARCHAR(1000),"
            " ADD COLUMN class VARCHAR(50) "
            )
        conn_new.commit()
    except:
        print('exception adding columns')
        c_new = conn_new.cursor()'''

    print('getting changed cluster ids...')
    tmp_filename = "temp_old_cluster_file.csv"
    with open(tmp_filename, 'w') as file_out:
        c.copy_expert("COPY (SELECT DISTINCT ON (ch.uuid) ch.uuid, cc.uuid as golden_uuid,  sector, note, class "
	    " FROM (SELECT cluster_id, uuid FROM contributions as c, processed_donors as d "
        " WHERE d.cluster_id != d.orig_cluster_id AND c.donor_id = d.donor_id) as ch,"
        " contributions as cc, processed_donors as pp"
        " WHERE ch.cluster_id = pp.donor_id AND cc.donor_id = pp.donor_id) TO STDOUT WITH CSV HEADER",file_out)
    
    print('creating temp cluster table...')
    c_new.execute("CREATE TEMP TABLE tmp_u"
             " (uuid VARCHAR(200), golden_uuid VARCHAR(200), sector VARCHAR(50),"
             " note VARCHAR(1000), class VARCHAR(50), new_cluster_id INTEGER, donor_id INTEGER) ")
    conn_new.commit()
    with open(tmp_filename, 'r+') as csv_file:
        c_new.copy_expert("COPY tmp_u "
        "(uuid, golden_uuid, sector, note, class) "
        "FROM STDIN CSV HEADER", csv_file)
    conn_new.commit()
    
    print('Updating temp with cluster_id')
    c_new.execute("UPDATE tmp_u as u SET new_cluster_id = p.cluster_id "
        " FROM contributions as c, processed_donors as p "
        " WHERE p.donor_id = c.donor_id AND c.uuid = u.golden_uuid")
    conn_new.commit()
    
    print('Updating temp with donor_id')
    c_new.execute("UPDATE tmp_u as u SET donor_id = p.donor_id "
        " FROM contributions as c, processed_donors as p "
        " WHERE p.donor_id = c.donor_id and c.uuid = u.uuid")
    conn_new.commit()

    print('Updating processed_donor with cluster_ids')
    c_new.execute("UPDATE processed_donors as p "
        " SET cluster_id = u.new_cluster_id, "
        " class = CASE WHEN p.class IS NULL THEN u.class ELSE p.class END, " 
        " note = CASE WHEN p.note IS NULL THEN u.note ELSE p.note END," 
        " sector = CASE WHEN p.sector IS NULL THEN u.sector ELSE p.sector END "
        " FROM tmp_u as u " 
        " WHERE p.donor_id = u.donor_id ")
    conn_new.commit()


    c.close()
    conn.close()

    c_new.close()
    conn_new.close()

    os.remove(tmp_filename)
  

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='carry_fwd_clusters')
    parser.add_argument('second_db', help='db connection info for newer records')
    args=parser.parse_args()
    carry_fwd_cluster(args.second_db)
    finish()