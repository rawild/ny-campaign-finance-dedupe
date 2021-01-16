#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

__Note:__ You will need to run `python init_postgres_db.py [filer_file] [filings_file] `
before running this script.

Runs a matching run against the initiated DB

"""
import os
import time
import logging
import locale
import itertools
import io
import csv
import argparse


import dj_database_url
import psycopg2
import psycopg2.extras

import dedupe
import numpy


from psycopg2.extensions import register_adapter, AsIs
register_adapter(numpy.int32, AsIs)
register_adapter(numpy.int64, AsIs)
register_adapter(numpy.float32, AsIs)
register_adapter(numpy.float64, AsIs)


class Readable(object):

    def __init__(self, iterator):

        self.output = io.StringIO()
        self.writer = csv.writer(self.output)
        self.iterator = iterator

    def read(self, size):

        self.writer.writerows(itertools.islice(self.iterator, size))

        chunk = self.output.getvalue()
        self.output.seek(0)
        self.output.truncate(0)

        return chunk

# Created after connection timeouts during the score call
def reconnect_record_pairs(start):
    blocking_table = 'blocking_map_'+type
    db_conf = dj_database_url.config()
    read_con = psycopg2.connect(database=db_conf['NAME'],
                                user=db_conf['USER'],
                                password=db_conf['PASSWORD'],
                                host=db_conf['HOST'],
                                port=db_conf['PORT'],
                                cursor_factory=psycopg2.extras.RealDictCursor)
    with read_con.cursor('pairs', cursor_factory=psycopg2.extensions.cursor) as read_cur:
        read_cur.execute("SELECT a.donor_id, "
                      "row_to_json((SELECT d FROM (SELECT a.city, "
                                                         "a.name, "
                                                         "a.zip, "
                                                         "a.state, "
                                                         "a.street) d)), "
                      "b.donor_id, "
                      "row_to_json((SELECT d FROM (SELECT b.city, "
                                                         "b.name, "
                                                         "b.zip, "
                                                         "b.state, "
                                                         "b.street) d)) "
               "FROM (SELECT DISTINCT l.donor_id AS east, r.donor_id AS west "
                     "FROM "+blocking_table+" AS l "
                     "INNER JOIN "+blocking_table+" AS r "
                     "USING (block_key) "
                     "WHERE l.donor_id < r.donor_id) ids "
               "INNER JOIN processed_donors a ON ids.east=a.donor_id "
               "INNER JOIN processed_donors b ON ids.west=b.donor_id")
    record_pairs(read_cur, start)

# yeilds record pairs from a given cursor result
def record_pairs(result_set, start=0):
    cursor = start
    try:
        for i, row in enumerate(result_set):
            cursor = i
            if i >=start:
                a_record_id, a_record, b_record_id, b_record = row
                record_a = (a_record_id, a_record)
                record_b = (b_record_id, b_record)

                yield record_a, record_b

                if i % 10000 == 0:
                    print(i)
    except:
        reconnect_record_pairs(cursor)

def cluster_ids(clustered_dupes):

    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for donor_id, score in zip(cluster, scores):
            yield donor_id, cluster_id, score

''' Main method for this module. Import into other modules to run the deduplication.
    params: 
    settings file  - string - required - does not need to exist
    training file -string - required - does not need to exist
    type - string - required - 'IND' or 'CORP'
'''

def run_dedupe(settings_file, training_file, type):
    start_time = time.time()
    print("dedupe file ",dedupe.__file__)
    # Set the database connection from environment variable using
    # [dj_database_url](https://github.com/kennethreitz/dj-database-url)
    # For example:
    #   export DATABASE_URL=postgres://user:password@host/mydatabase
    db_conf = dj_database_url.config()

    if not db_conf:
        raise Exception(
            'set DATABASE_URL environment variable with your connection, e.g. '
            'export DATABASE_URL=postgres://user:password@host/mydatabase'
        )

    read_con = psycopg2.connect(database=db_conf['NAME'],
                                user=db_conf['USER'],
                                password=db_conf['PASSWORD'],
                                host=db_conf['HOST'],
                                port=db_conf['PORT'],
                                cursor_factory=psycopg2.extras.RealDictCursor)

    write_con = psycopg2.connect(database=db_conf['NAME'],
                                 user=db_conf['USER'],
                                 password=db_conf['PASSWORD'],
                                 host=db_conf['HOST'],
                                 port=db_conf['PORT'])

    # We'll be using variations on this following select statement to pull
    # in campaign donor info.
    if type == 'IND':
        DONOR_SELECT = "SELECT donor_id, city, name, zip, state, street " \
                   "from processed_donors where person = 1 and name not like '%unitem%' "
    else:
        DONOR_SELECT = "SELECT donor_id, city, name, zip, state, street " \
                   "from processed_donors where person != 1 and name not like '%unitem%' "

    # Open Settings
    
    print('reading from ', settings_file)
    with open(settings_file, 'rb') as sf:
        deduper = dedupe.StaticDedupe(sf, num_cores=1)
    print(f'deduper predicates: {deduper.predicates}')
    
    blocking_table = 'blocking_map_'+type
    # ## Clustering
    entity_map_table="entity_map_"+type
    with write_con:
        with write_con.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS "+entity_map_table)

            print('creating entity_map database')
            
            cur.execute("CREATE TABLE "+entity_map_table+" "
                        "(donor_id INTEGER, canon_id INTEGER, "
                        " cluster_score FLOAT, PRIMARY KEY(donor_id))")

    read_con1 = psycopg2.connect(database=db_conf['NAME'],
                                user=db_conf['USER'],
                                password=db_conf['PASSWORD'],
                                host=db_conf['HOST'],
                                port=db_conf['PORT'],
                                cursor_factory=psycopg2.extras.RealDictCursor)
    with read_con1.cursor('pairs', cursor_factory=psycopg2.extensions.cursor) as read_cur:
        read_cur.execute("SELECT a.donor_id, "
                      "row_to_json((SELECT d FROM (SELECT a.city, "
                                                         "a.name, "
                                                         "a.zip, "
                                                         "a.state, "
                                                         "a.street) d)), "
                      "b.donor_id, "
                      "row_to_json((SELECT d FROM (SELECT b.city, "
                                                         "b.name, "
                                                         "b.zip, "
                                                         "b.state, "
                                                         "b.street) d)) "
               "FROM (SELECT DISTINCT l.donor_id AS east, r.donor_id AS west "
                     "FROM "+blocking_table+" AS l "
                     "INNER JOIN "+blocking_table+" AS r "
                     "USING (block_key) "
                     "WHERE l.donor_id < r.donor_id) ids "
               "INNER JOIN processed_donors a ON ids.east=a.donor_id "
               "INNER JOIN processed_donors b ON ids.west=b.donor_id")

        print('clustering...')
        clustered_dupes = deduper.cluster(deduper.score(record_pairs(read_cur)),
                                          threshold=0.5)

        # ## Writing out results

        # We now have a sequence of tuples of donor ids that dedupe believes
        # all refer to the same entity. We write this out onto an entity map
        # table

        print('writing results')
        write_con1 = psycopg2.connect(database=db_conf['NAME'],
                                 user=db_conf['USER'],
                                 password=db_conf['PASSWORD'],
                                 host=db_conf['HOST'],
                                 port=db_conf['PORT'])
        with write_con1:
            with write_con1.cursor() as write_cur:
                write_cur.copy_expert('COPY '+entity_map_table+' FROM STDIN WITH CSV',
                                      Readable(cluster_ids(clustered_dupes)),
                                      size=100000)

    with write_con1:
        with write_con1.cursor() as cur:
            cur.execute("CREATE INDEX head_index_"+type+" ON "+entity_map_table+" (canon_id)")

    # Print out the number of duplicates found

    # ## Payoff

    # With all this done, we can now begin to ask interesting questions
    # of the data
    #
    # For example, let's see who the top 10 donors are.

    locale.setlocale(locale.LC_ALL,'en_US.UTF-8')  # for pretty printing numbers
    read_con2 = psycopg2.connect(database=db_conf['NAME'],
                                user=db_conf['USER'],
                                password=db_conf['PASSWORD'],
                                host=db_conf['HOST'],
                                port=db_conf['PORT'],
                                cursor_factory=psycopg2.extras.RealDictCursor)
    # save entity map
    entity_map_filename = 'entity_map_' + settings_file.split('/')[-1] + '_' + time.strftime('%d_%m_%y_%H%M', time.localtime()) + '.csv'
    donors_filename = 'processed_donors_' + settings_file.split('/')[-1] + '_' + time.strftime('%d_%m_%y_%H%M', time.localtime()) + '.csv'
    with read_con2.cursor() as cur:
        with open(entity_map_filename, 'w') as file_out:
            cur.copy_expert('COPY '+entity_map_table+' TO STDOUT WITH CSV HEADER', file_out)
        with open(donors_filename, 'w') as file_out:
            cur.copy_expert('COPY processed_donors TO STDOUT WITH CSV HEADER', file_out)
    
    if type == 'IND':
        top_donor_where = 'donors.corp is null'
    else:
        top_donor_where = 'donors.corp is not null'
    # Create a temporary table so each group and unmatched record has
    # a unique id
    with read_con2.cursor() as cur:
        cur.execute("CREATE TEMPORARY TABLE e_map "
                    "AS SELECT COALESCE(canon_id, donor_id) AS canon_id, donor_id "
                    "FROM "+entity_map_table+" "
                    "RIGHT JOIN donors USING(donor_id)")

        cur.execute(
            "SELECT CONCAT_WS(' ', donors.first_name, donors.last_name, donors.corp) AS name, "
            "donation_totals.totals AS totals "
            "FROM (SELECT * FROM donors WHERE "+top_donor_where+") as donors INNER JOIN "
            "(SELECT canon_id, SUM(CAST(contributions.amount AS FLOAT)) AS totals "
            " FROM contributions INNER JOIN e_map "
            " USING (donor_id) "
            " GROUP BY (canon_id) "
            " ORDER BY totals "
            " DESC) "
            "AS donation_totals ON donors.donor_id=donation_totals.canon_id "
            "WHERE donors.donor_id = donation_totals.canon_id and donation_totals.totals IS NOT NULL LIMIT 10"
        )

        print("Top Donors (deduped)")
        for row in cur:
            row['totals'] = locale.currency(row['totals'], grouping=True)
            print('%(totals)20s: %(name)s' % row)

        # Compare this to what we would have gotten if we hadn't done any
        # deduplication
      
        cur.execute(
            "SELECT name, totals FROM (SELECT CONCAT_WS(' ', donors.first_name, donors.last_name, donors.corp) AS name, "
            "SUM(CAST(contributions.amount AS FLOAT)) AS totals "
            "FROM donors INNER JOIN contributions "
            "USING (donor_id) "
            "WHERE "+top_donor_where+" "
            "GROUP BY (donor_id) "
            "ORDER BY totals DESC) AS t where totals IS NOT NULL "
            "LIMIT 10"
        )

        print("Top Donors (raw)")
        for row in cur:
            row['totals'] = locale.currency(row['totals'], grouping=True)
            print('%(totals)20s: %(name)s' % row)

        cur.execute(
            " SELECT CONCAT_WS(' ', donors.first_name, donors.last_name, donors.corp) AS name, "
            " cluster_size, cluster_id "
            " FROM (SELECT * FROM donors WHERE "+top_donor_where+") as donors "
            " INNER JOIN (SELECT count(*) AS cluster_size, canon_id AS cluster_id "
            " FROM "+entity_map_table+" "
            " GROUP BY canon_id) "
            " AS cluster_totals "
            " ON donors.donor_id = cluster_totals.cluster_id "
            " ORDER BY cluster_size DESC LIMIT 10"
        )
        # ##Print stats that are saved to match_runs table
        # Biggest cluster first
        print("Biggest Clusters")
        biggest_name=''
        for row in cur:
            print('%(cluster_size)20s: %(name)s' % row)
            if biggest_name == '':
                biggest_name = row['name'] +':'+str(row['cluster_id'])
        # Then total of donors
        if type == 'IND':
            processed_donors_where = '= 1'
        else:
            processed_donors_where = '!= 1'
        cur.execute(
            " SELECT count(*) AS num_donors FROM processed_donors WHERE person " + processed_donors_where
        )
        for row in cur:
            number_of_donors = row['num_donors']
        # Then max, average and number of clusters
        cur.execute(
            " SELECT MAX(cluster_size) AS Biggest, AVG(cluster_size) AS Average, COUNT(cluster_id) AS number_of_clusters "
            " FROM (SELECT CONCAT_WS(' ', donors.first_name, donors.last_name, donors.corp) AS name, "
            " cluster_size, cluster_id "
            " FROM (SELECT * FROM donors WHERE "+top_donor_where+") as donors"
            " INNER JOIN (SELECT count(*) AS cluster_size, canon_id AS cluster_id "
            " FROM "+entity_map_table+" "
            " GROUP BY canon_id) "
            " AS cluster_totals "
            " ON donors.donor_id = cluster_totals.cluster_id "
            " ORDER BY cluster_size DESC) AS stats"
        )
        # Print the stats
        print("stats...")
        print(f"total number of donors: {number_of_donors}")
        for row in cur:
            print('max: %(biggest)s, average: %(average)s, number of clusters: %(number_of_clusters)s' % row)
            biggest_size = row['biggest']
            average_size = row['average']
            number_of_clusters = row['number_of_clusters']
    # write to the match_run table
    runtime = time.time() - start_time
    donor_cluster_ratio = number_of_donors/number_of_clusters
    with write_con1.cursor() as cur:
        cur.execute(""" 
            INSERT INTO match_runs 
            (completed, predicates, total_clusters, avg_cluster_size, biggest_cluster_size, biggest_cluster,
             total_donors, donor_type, total_run_time, donor_cluster_ratio, settings_file)
            VALUES (NOW(), %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s) """,
            (' '.join(str(pred) for pred in deduper.predicates), number_of_clusters, average_size, biggest_size, biggest_name,
            number_of_donors, type, runtime, donor_cluster_ratio, settings_file)
        )
    write_con1.commit()

    read_con.close()
    write_con.close()
    read_con1.close()
    write_con1.close()
    read_con2.close()
    print('ran in', runtime, 'seconds')


''' This is the main module for running the deduplication directly 
     To pass a previously generated settings file to the dedupe method use the -s flag
     run `python campaign_finance_dedupe.py -s setting_file_name`

     Dedupe uses Python logging to show or suppress verbose output. Added
     for convenience.  To enable verbose output, run `python
     campaign_finance_dedupe.py -v`

     There are two kinds of donors in the database corporations (CORP) and people (IND).
     Better matching is achieved when using different settings for each category.
     Specify which category with the -t flag. The default is CORP.
     run `python campaign_finance_dedupe.py -t IND`

     Multiple configurations can be used in combination like so:
     `python campaign_finance_dedupe.py -s settings_file_name -t IND -v-v`

    based on dedupe.io example:  
    https://github.com/dedupeio/dedupe-examples/tree/master/pgsql_big_dedupe_example
'''

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='run campaign finance dedupe')
    parser.add_argument('--settings_file', '-s', help='file to load previously generated settings from')
    parser.add_argument('--verbose', '-v', action='count',
                    help='Increase verbosity (specify multiple times for more)')
    parser.add_argument('--type', '-t', default='CORP', help='IND or CORP')
    args = parser.parse_args()
    log_level = logging.WARNING
    if args.verbose:
        if args.verbose == 1:
            log_level = logging.INFO
        elif args.verbose >= 2:
            log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    # ## Setup
    if args.type:
        type = args.type
    else:
        type = 'CORP'

    if args.settings_file:
        settings_file = args.settings_file
    else:
        settings_file = 'settings_'+ type + '_' + time.strftime('%d_%m_%y_%H%M', time.localtime())
    training_file = 'training_'+ type + '_' + time.strftime('%d_%m_%y_%H%M', time.localtime())+'.json' 

    
    # ## Run
    run_dedupe(settings_file, training_file, type)
