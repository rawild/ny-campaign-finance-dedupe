import dj_database_url
import psycopg2
import psycopg2.extras
import argparse
import os


def load_clusters(processed_donors, entity_map):
    # check if files exists
    if not os.path.exists(processed_donors):
        print('processed_donors file not found: %s' % processed_donors)
    
    if not os.path.exists(entity_map):
        print('entity_map file not found: %s' % entity_map)

    # get database connection info
    db_conf = dj_database_url.config()

    if not db_conf:
        raise Exception(
            'set DATABASE_URL environment variable with your connection, e.g. '
            'export DATABASE_URL=postgres://user:password@host/mydatabase'
        )

    # connect to database
    conn = psycopg2.connect(database=db_conf['NAME'],
                            user=db_conf['USER'],
                            password=db_conf['PASSWORD'],
                            host=db_conf['HOST'],
                            port=db_conf['PORT'])
    c = conn.cursor()

    # clear out tables
    c.execute("DROP TABLE IF EXISTS processed_donors")
    c.execute("DROP TABLE IF EXISTS entity_map")
    conn.commit()
    # create processed_donor table
    print('creating processed_donors...')
    c.execute("CREATE TABLE processed_donors "
            "(donor_id INTEGER, "
            "city VARCHAR(50), "
            "name VARCHAR(100), " 
            "zip VARCHAR(10), "
            "state VARCHAR(15), "
            "street VARCHAR(70), " 
            "person INTEGER)")
    c.execute("CREATE INDEX processed_donor_idx ON processed_donors (donor_id)")
    conn.commit()

    # write to processed_donor table from file
    with open(processed_donors, 'r') as csv_file:
        c.copy_expert(" COPY processed_donors "
                    " (donor_id, city, name, zip, "
                    " state, street, person) "
                    " FROM STDIN CSV HEADER ", csv_file)
    conn.commit()

    # create entity_map database
    print('creating entity_map database..')
    c.execute(" CREATE TABLE entity_map "
                " (donor_id INTEGER, canon_id INTEGER, "
                " cluster_score FLOAT, PRIMARY KEY(donor_id)) ")
    conn.commit()
    
    # write to entity_map from file
    with open(entity_map, 'r') as csv_file:
        c.copy_expert(" COPY entity_map "
                    "(donor_id, canon_id, cluster_score) "
                    "FROM STDIN CSV HEADER ", csv_file)
    conn.commit()

    # Finished?
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='cluster_data_load')
    parser.add_argument('processed_donors', help='saved processed donors from previous run')
    parser.add_argument('entity_map', help='saved entity map from previous run')
    args=parser.parse_args()
    load_clusters(args.processed_donors, args.entity_map)