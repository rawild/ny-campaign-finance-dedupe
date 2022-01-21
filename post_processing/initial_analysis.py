'''
Interesting queries to run immediately after new data load

Usage:
python3 run_initial_analysis.py

'''

import argparse, os
import dj_database_url
import psycopg2
import psycopg2.extras
import pandas as pd

def initial_analysis():
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
   
    top_donors_file = "top_donors_since_2021.csv"
    print(top_donors_file)
    with open(top_donors_file, 'w') as file_out:
        c.copy_expert("COPY (SELECT cluster_id, mode() WITHIN GROUP (ORDER BY d.name) as name, "
        " SUM(amount) as total "
    " FROM contributions as c, processed_donors as d "
    " WHERE c.donor_id = d.donor_id AND date >= '2021-01-01' "
    " GROUP BY cluster_id ORDER BY total DESC LIMIT 1000) TO STDOUT WITH CSV HEADER",file_out)

    top_recipients_of_top_donors_file = "top_recipients_of_top_donors_since_2021.csv"
    print(top_recipients_of_top_donors_file)
    with open(top_recipients_of_top_donors_file, 'w') as file_out:
        c.copy_expert("COPY (SELECT filer_id, mode() WITHIN GROUP (ORDER BY r.name), "
        " SUM(amount) as total_recieved FROM(select cluster_id, SUM(CAST(amount as double precision)) as total "
        " FROM contributions as c, processed_donors as d "
        " WHERE c.donor_id = d.donor_id AND date >= '2021-01-01' "
        " GROUP BY cluster_id ORDER BY total DESC LIMIT 1000) as td, "
        " contributions as cc, processed_donors as dd, recipients as r "
        " WHERE td.cluster_id = dd.cluster_id AND cc.donor_id = dd.donor_id "   
        " AND cc.recipient_id = r.filer_id AND date > '2021-01-01' and amount IS NOT null "
        " GROUP BY filer_id "
        " ORDER BY total_recieved DESC) TO STDOUT WITH CSV HEADER",file_out)

    top_recipients_file = "top_recipients_since_2021.csv"
    print(top_recipients_file)
    with open(top_recipients_file, 'w') as file_out:
        c.copy_expert("COPY (SELECT filer_id, mode() WITHIN GROUP (ORDER BY r.name), "
        " SUM(amount) as total_recieved FROM"
        " contributions as c, processed_donors as d, recipients as r"
        " WHERE c.donor_id = d.donor_id "
        " AND c.recipient_id = r.filer_id AND date > '2021-01-01' and amount IS NOT null "
        " GROUP BY filer_id ORDER BY total_recieved DESC LIMIT 1000) TO STDOUT WITH CSV HEADER",file_out)

    top_recipients_of_former_cuomo_donors_file = "top_recipients_of_former_cuomo_ind_donors_since_2021_01_01.csv"
    print(top_recipients_of_former_cuomo_donors_file)
    with open(top_recipients_of_former_cuomo_donors_file, 'w') as file_out:
        c.copy_expert("COPY (SELECT filer_id, mode() WITHIN GROUP (ORDER BY r.name), "
        " SUM(amount) as total_recieved FROM "
        " contributions as c, processed_donors as d, recipients as r "
        " WHERE c.donor_id = d.donor_id "
        " AND c.recipient_id = r.filer_id AND date > '2021-01-01' and amount IS NOT null "
        " AND class IS NOT NULL GROUP BY filer_id ORDER BY total_recieved DESC LIMIT 1000) TO STDOUT WITH CSV HEADER",file_out)

    top_recipients_of_former_cuomo_donors_file = "top_recipients_of_former_cuomo_ind_donors_since_2021_06_01.csv"
    print(top_recipients_of_former_cuomo_donors_file)
    with open(top_recipients_of_former_cuomo_donors_file, 'w') as file_out:
        c.copy_expert("COPY (SELECT filer_id, mode() WITHIN GROUP (ORDER BY r.name), "
        " SUM(amount) as total_recieved FROM "
        " contributions as c, processed_donors as d, recipients as r "
        " WHERE c.donor_id = d.donor_id "
        " AND c.recipient_id = r.filer_id AND date > '2021-06-01' and amount IS NOT null "
        " AND class IS NOT NULL GROUP BY filer_id ORDER BY total_recieved DESC LIMIT 1000) TO STDOUT WITH CSV HEADER",file_out)


def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='inital analysis')
    initial_analysis()
    finish()