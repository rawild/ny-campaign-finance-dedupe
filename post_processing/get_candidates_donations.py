import dj_database_url
import psycopg2
import psycopg2.extras
import argparse, os
import pandas as pd


'''
given a list of candidate ids and cluster ids, exports data for candidates and clusters
python3 get_candidates_donations.py ../../ecosoc_scorecard/candidates_for_download.csv ../../ecosoc_scorecard/cluster_ids_of_interest_no_generic_lobbyists.csv
'''

def get_candidates_donations(candidate_file,cluster_file):
    print('getting candidate csv for processing...')
    df = pd.read_csv(candidate_file, encoding='latin-1')
    df = df[['candidate_id']]
    df = df.astype({'candidate_id': int}, copy=False)
    df.to_csv('candidate_id_for_upload.csv', index=False)


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


    print('creating temp candidate table...')

    c.execute("CREATE TEMP TABLE tmp_can"
             "(candidate_id INTEGER) ")
    conn.commit()

    with open('candidate_id_for_upload.csv', 'r+') as csv_file:
        c.copy_expert("COPY tmp_can "
        "(candidate_id) "
        "FROM STDIN CSV HEADER", csv_file)
    conn.commit()
    
    if cluster_file != "":
        print('getting cluster csv for processing...')
        df = pd.read_csv(cluster_file, encoding='latin-1')
        df = df[['cluster_id']]
        df = df.astype({'cluster_id': int}, copy=False)
        df.to_csv('cluster_id_for_upload.csv', index=False)

        print('creating temp cluster table...')

        c.execute("CREATE TEMP TABLE tmp_clus"
                "(cluster_id INTEGER) ")
        conn.commit()

        with open('cluster_id_for_upload.csv', 'r+') as csv_file:
            c.copy_expert("COPY tmp_clus "
            "(cluster_id) "
            "FROM STDIN CSV HEADER", csv_file)
        conn.commit()

        print('outputting donations ...')
        out_filename = "candidates_donations_for_clusters.csv"
        with open(out_filename, 'w') as file_out:
            c.copy_expert("COPY ( SELECT cf.candidate_id, can.First_Name as cand_first, can.Last_Name as cand_last,"
            " c.amount, c.uuid, c.recipient_id, c.date, d.cluster_id, d.name as donor_name, d.city as donor_city, d.zip as donor_zip,"
            " d.state as donor_state, person, sector, class, note FROM contributions as c, processed_donors as d,"
            " candidate_filer as cf, candidates as can, tmp_can as tc, tmp_clus as tcl WHERE c.donor_id = d.donor_id AND"
            " CAST(cf.filer_id AS VARCHAR) = c.recipient_id AND cf.candidate_id = can.candidate_id "
            " AND d.cluster_id =tcl.cluster_id AND cf.candidate_id = tc.candidate_id )"
            " TO STDOUT WITH CSV HEADER",file_out)
        
        
        out_filename = "candidates_donations_summary.csv" 
        with open(out_filename, 'w') as file_out:
            c.copy_expert("COPY ( SELECT candidate_id, MODE() WITHIN GROUP (ORDER BY cand_first) as cand_first,"
            " MODE() WITHIN GROUP (ORDER BY cand_last) as cand_last, SUM(amount) FROM ( "
            " SELECT cf.candidate_id, can.First_Name as cand_first, can.Last_Name as cand_last,"
            " c.amount, c.uuid, c.recipient_id, c.date, d.name as donor_name, d.city as donor_city, d.zip as donor_zip,"
            " d.state as donor_state, person, sector, class, note FROM contributions as c, processed_donors as d,"
            " candidate_filer as cf, candidates as can, tmp_can as tc, tmp_clus as tcl WHERE c.donor_id = d.donor_id AND"
            " CAST(cf.filer_id AS VARCHAR) = c.recipient_id AND cf.candidate_id = can.candidate_id "
            " AND d.cluster_id =tcl.cluster_id AND cf.candidate_id = tc.candidate_id ) as k GROUP BY candidate_id)"
            " TO STDOUT WITH CSV HEADER",file_out)
        
        os.remove('cluster_id_for_upload.csv')
            

    else:
        print('outputting donations ...')
        out_filename = "candidates_donations.csv"
        with open(out_filename, 'w') as file_out:
            c.copy_expert("COPY ( SELECT cf.candidate_id, can.First_Name as cand_first, can.Last_Name as cand_last,"
            " c.amount, c.uuid, c.recipient_id, c.date, d.name as donor_name, d.city as donor_city, d.zip as donor_zip,"
            " d.state as donor_state, person, sector, class, note FROM contributions as c, processed_donors as d,"
            " candidate_filer as cf, candidates as can, tmp_can as tc WHERE c.donor_id = d.donor_id AND"
            " CAST(cf.filer_id AS VARCHAR) = c.recipient_id AND cf.candidate_id = can.candidate_id "
            " AND cf.candidate_id = tc.candidate_id )"
            " TO STDOUT WITH CSV HEADER",file_out)

    
    
    
    
    c.close()
    conn.close()




    os.remove('candidate_id_for_upload.csv')
  

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='get candidates donations')
    parser.add_argument('candidate_file', help='file with candidate ids (from db)')
    parser.add_argument('cluster_file', help='file with cluster ids (from db)')
    args=parser.parse_args()
    get_candidates_donations(args.candidate_file,args.cluster_file)
    finish()