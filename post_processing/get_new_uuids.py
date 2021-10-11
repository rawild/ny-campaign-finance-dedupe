'''
Carry fwd the human edit to the matching when new data is loaded.

Usage:
python3 get_new_uuids.py ../../carry_fwd/new_to_old_filer_id_map.csv ../../carry_fwd/uuids_for_fixing_part_1.csv 

'''
import argparse, os
import dj_database_url
import psycopg2
import psycopg2.extras
import pandas as pd


def get_new_uuids(new_filer_ids,uuids_for_fixing):
    print('getting new filer ids csv for processing...')
    df = pd.read_csv(new_filer_ids, dtype={0:'int',1:'str',}, encoding='latin-1')
    df.to_csv('new_filer_ids.csv', index=False)
    
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
             "(new_filer_id INTEGER, old_filer_id VARCHAR(12)) ")
    conn.commit()
    with open('new_filer_ids.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(new_filer_id, old_filer_id) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    print('getting uuid csv for processing...')
    df = pd.read_csv(uuids_for_fixing, dtype={0:'str',1:'str',2:'float',3:'int',4:'int'}, encoding='latin-1')
    df.to_csv('uuids_for_fixing.csv', index=False)
    
    for index,row in df.iterrows():
        if index % 100 == 0:
            print(index)
        uuid = row['uuid']
        if pd.isna(uuid):
            continue
        uuid_parts = str(uuid).split("-")
        if len(uuid_parts) < 4:
            df.at[index,'bad_uuid'] = 1
            continue
        date = str(uuid_parts[3])
        month = date[0:2]
        day = date[2:4]
        year = date[4:]
        old_filer_id = uuid_parts[0]
        new_filer_id_query = "SELECT new_filer_id FROM tmp_d WHERE old_filer_id = '" + old_filer_id + "'"
        c.execute(new_filer_id_query)
        #print("getting new_filer_id")
        new_id_record = c.fetchone()
        new_filer_id = new_id_record[0]
        new_uuid_mask = str(new_filer_id)+"-"+str(uuid_parts[1])+"-%-"+year+month+day
        #print("new uuid mask= ",new_uuid_mask)
        amount = row["amount"]
        if pd.isna(amount):
            amount = "NULL"
        new_uuids_query = "SELECT uuid FROM contributions as c,processed_donors as p "\
            "WHERE recipient_id = '"+str(new_filer_id)+"' AND type = '"+str(uuid_parts[1])+"' " \
            "AND date = '"+year+"-"+month+"-"+day+"'AND CAST(amount AS double precision) = "+str(amount)+" "\
            "AND c.donor_id = p.donor_id "
        c.execute(new_uuids_query)
        #print("getting new_uuid")
        new_uuids_result = c.fetchall()
        #print("length new_uuids= ", len(new_uuids_result))
        name = str(row["name"]).replace("'","").replace(",","")
        name_parts = name.split(" ")
        name_mask = name_parts[0]+"%"+name_parts[-1]
        if len(new_uuids_result) != 1:
            new_uuids_query_1 = "SELECT uuid FROM contributions as c,processed_donors as p "\
            "WHERE recipient_id = '"+str(new_filer_id)+"' AND type = '"+str(uuid_parts[1])+"' " \
            "AND date = '"+year+"-"+month+"-"+day+"'AND CAST(amount AS double precision) = "+str(amount)+" "\
            "AND c.donor_id = p.donor_id AND name LIKE '"+name_mask+"'"
            c.execute(new_uuids_query_1)
            new_uuids_result = c.fetchall()
            if len(new_uuids_result) != 1:
                new_uuids_query_2 = "SELECT uuid FROM contributions as c,processed_donors as p "\
                "WHERE recipient_id = '"+str(new_filer_id)+"' AND type = '"+str(uuid_parts[1])+"' " \
                "AND date = '"+year+"-"+month+"-"+day+"'AND CAST(amount AS double precision) = "+str(round(amount,1))+" "\
                "AND c.donor_id = p.donor_id AND name LIKE '"+name_mask+"'"
                c.execute(new_uuids_query_2)
                new_uuids_result = c.fetchall()
                if len(new_uuids_result) != 1:
                    new_uuids_query_3 = "SELECT uuid FROM contributions as c,processed_donors as p "\
                    "WHERE recipient_id = '"+str(new_filer_id)+"' AND type = '"+str(uuid_parts[1])+"' " \
                    "AND date = '"+year+"-"+month+"-"+day+"'AND CAST(amount AS double precision) = "+str(round(amount))+" "\
                    "AND c.donor_id = p.donor_id AND name LIKE '"+name_mask+"'"
                    c.execute(new_uuids_query_3)
                    new_uuids_result = c.fetchall()
                    if len(new_uuids_result) != 1:
                    df.at[index,'bad_result'] = len(new_uuids_result)
                    continue

        for result in new_uuids_result:
            #print("uuid =", result[0])
            df.at[index,'new_uuid'] = result[0]

    df.to_csv('uuids_with_new_uuids.csv')
    c.close()
    conn.close()

    os.remove('new_filer_ids.csv')
    os.remove('uuids_for_fixing.csv')

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='carry_fwd_clusters')
    parser.add_argument('new_filer_ids', help='file with new filer ids')
    parser.add_argument('uuids_for_fixing', help='file of uuids to fix')
    args=parser.parse_args()
    get_new_uuids(args.new_filer_ids,args.uuids_for_fixing)
    finish()