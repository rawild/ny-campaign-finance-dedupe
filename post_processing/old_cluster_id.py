'''
Utils for looking up old cluster_id to new one-- THIS IS FOR DATA DOWNLOADED BEFORE January 2021

Usage:
python3 old_cluster_id.py 'postgres://user:password@host/mydatabase' ../../carry_fwd/new_to_old_filer_id_map.csv ../../Cuomo_TTR_escalation/CuomoTopDonors_for_sector_upload.csv 

'''

import argparse, os
import dj_database_url
import psycopg2
import psycopg2.extras
import pandas as pd
from decimal import Decimal,ROUND_UP



def get_new_cluster_ids(second_db,new_filer_ids, old_cluster_ids):
    print('getting new filer ids csv for processing...')
    df = pd.read_csv(new_filer_ids, dtype={0:'int',1:'str',}, encoding='latin-1')
    df.to_csv('new_filer_ids.csv', index=False)
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

    conn2=psycopg2.connect(database=db_conf_new['NAME'],
                            user=db_conf_new['USER'],
                            password=db_conf_new['PASSWORD'],
                            host=db_conf_new['HOST'],
                            port=db_conf_new['PORT'])
    
    c2 = conn2.cursor()
    
    print('creating temp table...')

    c.execute("CREATE TEMP TABLE tmp_d"
             "(new_filer_id INTEGER, old_filer_id VARCHAR(12)) ")
    conn.commit()
    with open('new_filer_ids.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(new_filer_id, old_filer_id) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()

    print('getting old_cluster_id csv for processing...')
    df = pd.read_csv(old_cluster_ids,  encoding='latin-1')
    
    for index,row in df.iterrows():
        if index % 100 == 0:
            print(index)
        
        cluster_id = row['cluster_id']
        if pd.isna(cluster_id):
            continue
        orig_uuid_query = "SELECT distinct(uuid),amount from contributions as c,processed_donors as p where p.cluster_id=c.donor_id AND cluster_id = '"+str(int(cluster_id))+"'"
        c.execute(orig_uuid_query)
        orig_uuid_record = c.fetchone()
        if pd.isna(orig_uuid_record):
            continue
        orig_uuid = orig_uuid_record[0]
        #print("orig_uuid: "+orig_uuid)
        df.at[index,'new_uuid'] = orig_uuid
        amount = orig_uuid_record[1]
        #print("orig amount: "+amount)
        #print("int amount: "+str(int(float(amount))))
        if pd.isna(orig_uuid):
            continue
        uuid_parts = str(orig_uuid).split("-")
        if len(uuid_parts) < 4:
            df.at[index,'bad_uuid'] = 1
            continue
        date = str(uuid_parts[3])
        month = date[0:2]
        day = date[2:4]
        year = date[4:]
        if int(year) < 1990:
            if len(year) == 4:
                year = "20"+year[2:]
            if len(year) == 3:
                year = "200"+year[2:]
        old_filer_id = uuid_parts[0]
        new_filer_id_query = "SELECT new_filer_id FROM tmp_d WHERE old_filer_id = '" + old_filer_id + "'"
        c.execute(new_filer_id_query)
        #print("getting new_filer_id")
        new_id_record = c.fetchone()
        if len(new_id_record) == 0:
            continue
        new_filer_id = new_id_record[0]

       
        if pd.isna(amount):
            amount = "NULL"
        new_uuids_query = "SELECT uuid FROM contributions as c,processed_donors as p "\
            "WHERE recipient_id = '"+str(new_filer_id)+"' AND type = '"+str(uuid_parts[1])+"' " \
            "AND date = '"+year+"-"+month+"-"+day+"' AND amount = '"+str(int(float(amount)))+"' "\
            "AND c.donor_id = p.donor_id "
        c2.execute(new_uuids_query)
        #print("getting new_uuid")
        new_uuids_result = c2.fetchall()
        #print("length new_uuids= ", len(new_uuids_result))
        name = str(row["name"]).replace("'","")
        name = name.replace(",","")
        name_parts = name.split(" ")
        if len(name_parts) > 1 :
            name_mask = name_parts[0]+"%"+name_parts[-1]
        else:
            name_mask = name
 
        if len(new_uuids_result) != 1:
            new_uuids_query_1 = "SELECT uuid FROM contributions as c,processed_donors as p "\
            "WHERE recipient_id = '"+str(new_filer_id)+"' AND type = '"+str(uuid_parts[1])+"' " \
            "AND date = '"+year+"-"+month+"-"+day+"' AND amount = '"+str(int(float(amount)))+"' "\
            "AND c.donor_id = p.donor_id AND name LIKE LOWER('"+name_mask+"')"
            c2.execute(new_uuids_query_1)
            new_uuids_result = c2.fetchall()
            #print("name mask "+name_mask)
            #print("length new_uuids= ", len(new_uuids_result))
            if len(new_uuids_result) == 0:
                if str(amount) == 'NULL':
                    df.at[index,'bad_result'] = len(new_uuids_result)
                    continue
                amount_round = Decimal(amount).quantize(Decimal('.1'), rounding=ROUND_UP)
                new_uuids_query_2 = "SELECT uuid FROM contributions as c,processed_donors as p "\
                "WHERE recipient_id = '"+str(new_filer_id)+"' AND type = '"+str(uuid_parts[1])+"' " \
                "AND date = '"+year+"-"+month+"-"+day+"' AND CAST(amount AS double precision) = "+str(amount_round)+" "\
                "AND c.donor_id = p.donor_id AND name LIKE LOWER('"+name_mask+"')"
                c2.execute(new_uuids_query_2)
                new_uuids_result = c2.fetchall()
                #print("amount round: "+str(amount_round))
                #print("length new_uuids= ", len(new_uuids_result))
                if len(new_uuids_result) == 0:
                    new_uuids_query_3 = "SELECT uuid FROM contributions as c,processed_donors as p "\
                    "WHERE recipient_id = '"+str(new_filer_id)+"' AND type = '"+str(uuid_parts[1])+"' " \
                    "AND date = '"+year+"-"+month+"-"+day+"' AND CAST(amount AS double precision) = "+str(int(round(float(amount),-1)))+" "\
                    "AND c.donor_id = p.donor_id AND name LIKE LOWER('"+name_mask+"')"
                    c2.execute(new_uuids_query_3)
                    new_uuids_result = c2.fetchall()
                    #print("round float: "+str(round(float(amount))))
                    #print("length new_uuids= ", len(new_uuids_result))
                    if len(new_uuids_result) == 0:
                        df.at[index,'bad_result'] = len(new_uuids_result)
                        df.at[index,'new_filer_id'] = new_filer_id
                        continue

        if len(new_uuids_result) > 0:
            result = new_uuids_result[0]
            #print("uuid =", result[0])
            new_uuid = result[0]
            df.at[index,'new_uuid'] = new_uuid
            new_cluster_id_query = "SELECT cluster_id from contributions as c, processed_donors as d WHERE c.donor_id = d.donor_id AND uuid = '"+new_uuid+"'"
            c2.execute(new_cluster_id_query)
            new_cluster_id_record = c2.fetchone()
            new_cluster_id = new_cluster_id_record[0]
            df.at[index,'new_cluster_id'] = new_cluster_id

    df.to_csv('cluster_ids_with_new_cluster_ids.csv')
    c.close()
    conn.close()

    c2.close()
    conn2.close()

    os.remove('new_filer_ids.csv')

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='carry_fwd_clusters')
    parser.add_argument('second_db', help='db connection info for newer records')
    parser.add_argument('new_filer_ids', help='file with new filer ids')
    parser.add_argument('old_cluster_ids', help='file of donor_ids for maping')
    args=parser.parse_args()
    get_new_cluster_ids(args.second_db,args.new_filer_ids,args.old_cluster_ids)
    finish()