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


def carry_fwd_cluster_sector(second_db):
    
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
    try:
        print('adding sector class and note columns')
        c_new.execute("ALTER TABLE processed_donors"
            " ADD COLUMN sector VARCHAR(50),"
            " ADD COLUMN note VARCHAR(1000),"
            " ADD COLUMN class VARCHAR(50) "
            )
        conn_new.commit()
    except:
        print('exception adding columns')

    print('getting changed cluster ids...')
    tmp_filename = "temp_old_cluster_file.csv"
    with open(tmp_filename, 'w') as file_out:
        c.copy_expert("COPY (SELECT DISTINCT ON (ch.uuid) ch.uuid, cc.uuid as golden_uuid,  sector, note, class "
	    " FROM (SELECT cluster_id, uuid FROM contributions as c, processed_donors as d "
        " WHERE d.cluster_id != d.orig_cluster_id AND c.donor_id = d.donor_id) as ch,"
        " contributions as cc, processed_donors as pp"
        " WHERE ch.cluster_id = pp.donor_id AND cc.donor_id = pp.donor_id) TO STDOUT WITH CSV HEADER",file_out)
    

    print('iterating through file and updating cluster ids...')

    df= pd.read_csv(tmp_filename)
    for index,row in df.iterrows():
        if index % 100 == 0:
            print(index)
        new_cluster_id_query = "SELECT cluster_id from contributions as c, processed_donors as p WHERE p.donor_id = c.donor_id and uuid = '"+row['golden_uuid']+"'"
        print(new_cluster_id_query)
        c_new.execute(new_cluster_id_query)
        new_cluster_id_record = c_new.fetchone()
        if pd.isna(new_cluster_id_record):
            continue
        new_cluster_id = new_cluster_id_record[0]
        print(new_cluster_id)
        donor_id_query = "SELECT donor_id from contributions as c, processed_donors as p WHERE p.donor_id = c.donor_id and uuid = '"+row['uuid']+"'"
        c_new.execute(donor_id_query)
        donor_id_record = c_new.fetchone()
        if pd.isna(donor_id_record):
            continue
        donor_id = donor_id_record[0]
        print(donor_id)
        c_new.execute( "UPDATE processed_donors as p"
            " SET cluster_id = "+new_cluster_id+
            " class = CASE WHEN p.class IS NULL THEN '"+row['class']+"' ELSE p.class END, "
            " note = CASE WHEN p.note IS NULL THEN '"+row['note']+"' ELSE p.note END,"
            " sector = CASE WHEN p.sector IS NULL THEN '"+row['sector']+"' ELSE p.sector END"
            " WHERE p.donor_id="+donor_id)
        conn_new.commit()
        print("finished round")

    print('getting sector info...')
    tmp_filename_1 = "temp_old_sector_file.csv"
    with open(tmp_filename_1, 'w') as file_out:
        c.copy_expert("COPY (SELECT uuid, sector, note, class FROM contributions as c, processed_donors as d "
    " WHERE d.sector IS NOT null and c.donor_id = d.donor_id) TO STDOUT WITH CSV HEADER",file_out)

    print('iterating through file and updating sector info ...')
    
    df= pd.read_csv(tmp_filename_1)
    for index,row in df.iterrows():
        if index % 100 == 0:
            print(index)
        donor_id_query = "SELECT donor_id from contributions as c, processed_donors as p WHERE p.donor_id = c.donor_id and uuid = '"+row['uuid']+"'"
        c_new.execute(donor_id_query)
        donor_id_record = c_new.fetchone()
        if pd.isna(donor_id_record):
            continue
        donor_id = donor_id_record[0]
        c_new.execute( "UPDATE processed_donors as p"
            " SET class = CASE WHEN p.class IS NULL THEN '"+row['class']+"' ELSE p.class END, "
            " note = CASE WHEN p.note IS NULL THEN '"+row['note']+"' ELSE p.note END,"
            " sector = CASE WHEN p.sector IS NULL THEN '"+row['sector']+"' ELSE p.sector END"
            " WHERE p.donor_id="+donor_id)
        conn_new.commit()

    c.close()
    conn.close()

    c_new.close()
    conn_new.close()

    os.remove(tmp_filename)
    os.remove(tmp_filename_1)

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='carry_fwd_clusters')
    parser.add_argument('second_db', help='db connection info for newer records')
    args=parser.parse_args()
    carry_fwd_cluster_sector(args.second_db)
    finish()