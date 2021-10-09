'''
Carry fwd the human edit to the matching when new data is loaded.
'''
import argparse, os
import dj_database_url
import psycopg2
import psycopg2.extras
import pandas as pd


def upload_fixed_cluster_ids(fixed_clusters_file):
    print('getting csv for processing...')
    df = pd.read_csv(fixed_clusters_file, dtype={0:'int',1:'int',2:'Int64'}, encoding='latin-1')
    df = df[['orig_cluster_id','donor_id', 'new_cluster_id']]
    df.to_csv('fixed_clusters_for_upload.csv', index=False)
    
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
             "(orig_cluster_id INTEGER, donor_id INTEGER, "
             "new_cluster_id INTEGER) ")
    conn.commit()
    with open('fixed_clusters_for_upload.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(orig_cluster_id, donor_id, new_cluster_id) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    print('updating processed_donors...')
    c.execute("UPDATE processed_donors as p"
            " SET cluster_id = new_cluster_id, "
            " updated = NOW() "
            " FROM tmp_d as t"
            " WHERE p.donor_id = t.donor_id AND t.new_cluster_id IS NOT NULL")
    conn.commit()
    
    c.close()
    conn.close()

    os.remove('fixed_clusters_for_upload.csv')
    

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='upload_fixed_clusters')
    parser.add_argument('fixed_clusters_file', help='file with fixed clusters')
    args=parser.parse_args()
    upload_fixed_cluster_ids(args.fixed_clusters_file)
    finish()