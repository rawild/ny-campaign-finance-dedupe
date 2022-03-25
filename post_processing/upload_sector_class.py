import dj_database_url
import psycopg2
import psycopg2.extras
import argparse, os
import pandas as pd


def upload_sector_class(sector_class_file, overwrite):
    print('getting csv for processing...')
    df = pd.read_csv(sector_class_file, encoding='latin-1')
    df = df[['cluster_id','class', 'note','sector']]
    df = df.astype({'cluster_id': int}, copy=False)
    df.to_csv('sector_class_for_upload.csv', index=False)


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
             "(cluster_id INTEGER, class VARCHAR(50), "
             " note VARCHAR(1000), sector VARCHAR(50)) ")
    conn.commit()

    with open('sector_class_for_upload.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_d "
        "(cluster_id, class, note, sector) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    print('updating processed_donors...')
    if overwrite != True:
        
        c.execute("UPDATE processed_donors as p"
            " SET class = CASE WHEN p.class IS NULL THEN t.class ELSE p.class END, "
            " note = CASE WHEN p.note IS NULL THEN t.note ELSE p.note END,"
            " sector = CASE WHEN p.sector IS NULL THEN t.sector ELSE p.sector END"
            " FROM tmp_d as t"
            " WHERE p.cluster_id = t.cluster_id ")
        conn.commit()
    else:
        c.execute("UPDATE processed_donors as p"
            " SET class = t.class, "
            " note = t.note, "
            " sector = t.sector "
            " FROM tmp_d as t"
            " WHERE p.cluster_id = t.cluster_id ")
        conn.commit()
    
    c.close()
    conn.close()

    os.remove('sector_class_for_upload.csv')

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='upload_sector_class')
    parser.add_argument('sector_class_file', help='file with billionaires and millionaires and sector')
    parser.add_argument('overwrite', help='overwrite existing sector')
    args=parser.parse_args()
    upload_sector_class(args.sector_class_file, args.overwrite)
    finish()