'''
Carry fwd the human edit to the matching when new data is loaded - same UUID

Usage:
python3 carry_fwd_sector.py "export DATABASE_URL='postgresql://username:password@db-url:port/db?sslmode=require'"

'''

import argparse, os
import dj_database_url
import psycopg2
import psycopg2.extras
import pandas as pd


def carry_fwd_sector(second_db):
    
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
    
    print('getting sector info...')
    tmp_filename_1 = "temp_old_sector_file.csv"
    with open(tmp_filename_1, 'w') as file_out:
        c.copy_expert("COPY (SELECT uuid, sector, note, class FROM contributions as c, processed_donors as d "
    " WHERE d.sector IS NOT null and c.donor_id = d.donor_id) TO STDOUT WITH CSV HEADER",file_out)

    print('creating temp sector table...')
    c_new.execute("CREATE TEMP TABLE tmp_s"
             " (uuid VARCHAR(200), sector VARCHAR(50),"
             " note VARCHAR(1000), class VARCHAR(50), donor_id INTEGER) ")
    conn_new.commit()
    with open(tmp_filename_1, 'r+') as csv_file:
        c_new.copy_expert("COPY tmp_s "
        "(uuid, sector, note, class) "
        "FROM STDIN CSV HEADER", csv_file)
    conn_new.commit()

    print('Updating temp sector table with donor_id')
    c_new.execute("UPDATE tmp_s as s SET donor_id = p.donor_id "
        " FROM contributions as c, processed_donors as p "
        " WHERE p.donor_id = c.donor_id and c.uuid = s.uuid")
    conn_new.commit()

    tmp_filename_2 = "temp_old_sector_file_donorid.csv"
    with open(tmp_filename_2, 'w') as file_out:
        c_new.copy_expert("COPY (SELECT * FROM tmp_s) TO STDOUT WITH CSV HEADER",file_out)


    print('Updating processed_donor with sectors')
    c_new.execute("UPDATE processed_donors as p "
        " SET class = CASE WHEN p.class IS NULL THEN s.class ELSE p.class END, " 
        " note = CASE WHEN p.note IS NULL THEN s.note ELSE p.note END," 
        " sector = CASE WHEN p.sector IS NULL THEN s.sector ELSE p.sector END "
        " FROM tmp_s as s " 
        " WHERE p.donor_id = s.donor_id ")
    conn_new.commit()

    c.close()
    conn.close()

    c_new.close()
    conn_new.close()

    #os.remove(tmp_filename_1)

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='carry_fwd_clusters')
    parser.add_argument('second_db', help='db connection info for newer records')
    args=parser.parse_args()
    carry_fwd_sector(args.second_db)
    finish()