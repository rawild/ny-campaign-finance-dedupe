'''
Carry fwd the human edit to the matching when new data is loaded.

Usage:

'''
import argparse, os
import dj_database_url
import psycopg2
import psycopg2.extras
import pandas as pd


def carry_fwd_clusters(golden_record, new_uuids):
    print('getting golden record csv for processing...')
    df = pd.read_csv(golden_record, dtype={0:'str',2:'str'}, encoding='latin-1')
    df.to_csv('conversion_uuids_old.csv', index=False)
    
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
             "(old_uuid VARCHAR(200), old_golden_uuid VARCHAR(200)) ")
    conn.commit()
    with open('conversion_uuids_old.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(old_uuid, old_golden_uuid) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    print('getting new_uuids csv for processing...')
    df = pd.read_csv(new_uuids, dtype={0:'str',1:'str',2:'float',3:'int',4:'int',5:'str'}, encoding='latin-1')
    df = df[['uuid','new_uuid']]
    df.to_csv('new_uuids_cut.csv', index = False)

    print('creating temp table...')

    c.execute("CREATE TEMP TABLE tmp_m"
             "(old_uuid VARCHAR(200), new_uuid VARCHAR(200)) ")
    conn.commit()
    with open('new_uuids_cut.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_m "
        "(old_uuid, new_uuid) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()

    error_dict = {'bad_uuid':[],'error': []}

    for index,row in df.iterrows():
        if index % 10 == 0:
            print(index)
        #print("old_golden_uuid_query")
        if type(row['uuid']) is not str :
            continue
        query_for_old_golden_uuid = "SELECT old_golden_uuid FROM tmp_d WHERE old_uuid = '"+row['uuid']+"'"
        c.execute(query_for_old_golden_uuid)
        old_golden_record = c.fetchone()
        if pd.isna(old_golden_record):
            error_dict['bad_uuid'].append(row['uuid'])
            error_dict['error'].append('no old golden found')
            continue
        old_golden_uuid = old_golden_record[0]
        if pd.isna(old_golden_uuid):
            error_dict['bad_uuid'].append(row['uuid'])
            error_dict['error'].append('no old golden uuid found')
            continue
        #print("new_golden_uuid_query")
        query_for_new_golden_uuid = "SELECT new_uuid FROM tmp_m WHERE old_uuid = '"+old_golden_uuid+"'"
        c.execute(query_for_new_golden_uuid)
        new_golden_record = c.fetchone()
        if pd.isna(new_golden_record):
            error_dict['bad_uuid'].append(row['uuid'])
            error_dict['error'].append('no new golden found')
            continue
        new_golden_uuid = new_golden_record[0]
        #print("golden_cluster_id_query")
        query_for_cluster_id = "SELECT cluster_id from contributions as c, processed_donors as d WHERE uuid = '"+new_golden_uuid+"'"\
        " AND c.donor_id = d.donor_id"
        c.execute(query_for_cluster_id)
        cluster_id_record = c.fetchone()
        if pd.isna(cluster_id_record):
            error_dict['bad_uuid'].append(row['uuid'])
            error_dict['error'].append('no cluster_id found')
            continue
        cluster_id = cluster_id_record[0]
        #print("donor_id_query")
        query_for_donor_id = "SELECT donor_id from contributions as c WHERE uuid = '"+ row['new_uuid']+"'"
        c.execute(query_for_donor_id)
        donor_id_record = c.fetchone()
        if pd.isna(donor_id_record):
            error_dict['bad_uuid'].append(row['uuid'])
            error_dict['error'].append('no donor id found')
            continue
        donor_id = donor_id_record[0]
        #print("update cluster id query")
        update_cluster_id = "UPDATE processed_donors SET cluster_id = '"+str(cluster_id)+"'" \
        " WHERE donor_id = '"+ str(donor_id) +"'"
        c.execute(update_cluster_id)
        conn.commit()

    c.close()
    conn.close()

    df_error = pd.DataFrame(data=error_dict)
    df_error.to_csv(new_uuids.split("/")[-1].split(".")[-2]+'_errored_records.csv', index=False)

    os.remove('conversion_uuids_old.csv')
    os.remove('new_uuids_cut.csv')
    

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='carry_fwd_clusters')
    parser.add_argument('new_filer_ids', help='file with new filer ids')
    parser.add_argument('uuids_for_fixing', help='file of uuids to fix')
    args=parser.parse_args()
    carry_fwd_clusters(args.new_filer_ids,args.uuids_for_fixing)
    finish()