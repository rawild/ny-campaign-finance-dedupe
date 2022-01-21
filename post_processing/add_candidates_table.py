import dj_database_url
import psycopg2
import psycopg2.extras
import argparse


def add_candidates(candidates_file):
   
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


    print('creating  table...')
    c.execute("CREATE TABLE candidates "
             "(candidate_id SERIAL PRIMARY KEY, first_name VARCHAR(50), middle_name VARCHAR(50), "
             "last_name VARCHAR(50), district INTEGER, party VARCHAR(50), office VARCHAR(50)) ")
    conn.commit()

    with open(candidates_file, 'r+') as csv_file:
        c.copy_expert("COPY candidates "
        "(first_name, middle_name, last_name, district, party, office) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    
    c.close()
    conn.close()



def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='file with candidates')
    parser.add_argument('candidates_file', help='file candidates in it')
    args=parser.parse_args()
    add_candidates(args.candidates_file)
    finish()