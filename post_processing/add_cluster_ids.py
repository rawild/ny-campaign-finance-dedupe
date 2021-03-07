'''
The beginning of figuring out how to add in human edit to the machine matching
'''
import argparse
import dj_database_url
import psycopg2
import psycopg2.extras



def add_cluster_ids():
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
    
    print('adding columns to processed_donors...')
    c.execute("ALTER TABLE processed_donors "
               "ADD orig_cluster_id INTEGER, "
               "ADD cluster_id INTEGER, "
               "ADD updated TIMESTAMP")
    conn.commit()
    
    print('copying over IND clusters to processed_donors...')
    c.execute("UPDATE processed_donors as p "
            " SET orig_cluster_id = canon_id "
            " FROM entity_map as e"
            " WHERE p.donor_id = e.donor_id")
    conn.commit()
    
    print('copying over CORP clusters to processed_donors...')
    c.execute("UPDATE processed_donors as p"
            " SET orig_cluster_id = canon_id "
            " FROM entity_map_corp as e"
            " WHERE p.donor_id = e.donor_id")
    conn.commit()
    
    print('updating empty orig_cluster_ids to be donor_id')
    c.execute("UPDATE processed_donors "
            " SET orig_cluster_id = donor_id")
    conn.commit()

    print('copying orig_cluster_ids to cluster_id')
    c.execute("UPDATE processed_donors "
            " SET cluster_id = orig_cluster_id, "
            " updated = NOW()")
    conn.commit()
    c.close()
    conn.close()

def finish():
    print("Finished")

if __name__ == '__main__':
    add_cluster_ids()
    finish()