import dj_database_url
import psycopg2
import psycopg2.extras
import argparse


def upload_fixed_amount(fixed_amount_file):
    print('getting csv for processing...')


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
             "(contribution_id INTEGER, amount VARCHAR(50)) ")
    conn.commit()

    with open(fixed_amount_file, 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(contribution_id, amount) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    print('updating contributions...')
    c.execute("UPDATE contributions as c"
            " SET amount = t.amount "
            " FROM tmp_d as t"
            " WHERE c.contribution_id = t.contribution_id ")
    conn.commit()
    
    c.close()
    conn.close()

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='upload_fixed_amount_info')
    parser.add_argument('fixed_amount_file', help='file with fixed contribution amounts')
    args=parser.parse_args()
    upload_fixed_amount(args.fixed_amount_file)
    finish()