import dj_database_url
import psycopg2
import psycopg2.extras
import argparse, os
import pandas as pd


def get_cluster_from_name(donors_name_file):
    print('getting name csv for processing...')
    df = pd.read_csv(donors_name_file, encoding='latin-1')
    df.to_csv('donors_name_for_upload.csv', index=False)


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
             "(name VARCHAR(250)) ")
    conn.commit()

    with open('donors_name_for_upload.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(name) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    print('selecting from processed_donors...')
    out_filename = "name_to_cluster_ids.csv"
    with open(out_filename, 'w') as file_out:
        c.copy_expert("COPY (SELECT cluster_id, Count(donor_id) AS cluster_size,  MODE() WITHIN GROUP (ORDER BY p.name), MODE() WITHIN GROUP (ORDER BY street), "
        " MODE() WITHIN GROUP (ORDER BY city), MODE() WITHIN GROUP (ORDER BY zip), MIN(donor_id), "
        " MODE() WITHIN GROUP (ORDER BY person), MODE() WITHIN GROUP (ORDER BY sector), MODE() WITHIN GROUP (ORDER BY note), "
        " MODE() WITHIN GROUP (ORDER BY class), MODE() WITHIN GROUP (ORDER BY orig_cluster_id) "
	    " FROM processed_donors as p, tmp_d as n WHERE LOWER(p.name) = LOWER(n.name) GROUP BY cluster_id ) TO STDOUT WITH CSV HEADER",file_out)
    
    c.close()
    conn.close()

    os.remove('donors_name_for_upload.csv')

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='get_cluster_from_name')
    parser.add_argument('donors_name_file', help='file with donors name')
    args=parser.parse_args()
    get_cluster_from_name(args.donors_name_file)
    finish()