'''
To make golden records line up need to have the old UUID for this new contributions
'''
import argparse
import dj_database_url
import psycopg2
import psycopg2.extras
import pandas as pd


def clean_duplicate_uuids():
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
    
    print('clearing duplicate uuids')
    c.execute("DELETE FROM contributions AS m "
                "USING ( "
                "SELECT * FROM ("
                "SELECT count(*) AS num, uuid, max(contribution_id) "
                "FROM contributions AS c "
                "GROUP BY uuid ) AS u WHERE num>1 ) AS d " 
                "WHERE m.uuid = d.uuid AND contribution_id < max")
    conn.commit()
    c.close()
    conn.close()

def finish():
    print("Finished")

if __name__ == '__main__':
    clean_duplicate_uuids()
    finish()