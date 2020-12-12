import dj_database_url
import psycopg2

def create_match_runs():
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
    print('creating match_runs table...')
    c.execute("CREATE TABLE match_runs "
            "(run_id SERIAL PRIMARY KEY, completed TIMESTAMP, "
            " predicates VARCHAR(1000), total_clusters INT, "
            " avg_cluster_size FLOAT, biggest_cluster_size INT, biggest_cluster VARCHAR(100), "
            " total_donors INT, donor_type VARCHAR(5), total_run_time FLOAT)")

    conn.commit()
    c.close()
    conn.close()
   
'''small module to create the match runs table in advance of a settings run'''
if __name__ == '__main__':
    create_match_runs()
    print('Finished')