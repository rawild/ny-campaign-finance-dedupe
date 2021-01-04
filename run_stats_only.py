import dj_database_url
import psycopg2
import psycopg2.extras
import time
import argparse
import locale
from matching_evaluation.combine_predicates import (get_predicates)

def run_stats(type, settings_file):
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
    
    locale.setlocale(locale.LC_ALL,'en_US.UTF-8')  # for pretty printing numbers

    if type == 'IND':
        top_donor_where = 'donors.corp is null'
    else:
        top_donor_where = 'donors.corp is not null'
    # Create a temporary table so each group and unmatched record has
    # a unique id
    with read_con.cursor() as cur:
        cur.execute("CREATE TEMPORARY TABLE e_map "
                    "AS SELECT COALESCE(canon_id, donor_id) AS canon_id, donor_id "
                    "FROM entity_map "
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
            " FROM entity_map "
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
            " FROM entity_map "
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
    #runtime = time.time() - start_time
    donor_cluster_ratio = number_of_donors/number_of_clusters
    predicates = get_predicates(settings_file)
    with write_con.cursor() as cur:
        cur.execute(""" 
            INSERT INTO match_runs 
            (completed, predicates, total_clusters, avg_cluster_size, biggest_cluster_size, biggest_cluster,
             total_donors, donor_type, donor_cluster_ratio, settings_file)
            VALUES (NOW(), %s, %s, %s, %s, %s,
            %s, %s, %s, %s) """,
            (' '.join(str(pred) for pred in predicates), number_of_clusters, average_size, biggest_size, biggest_name,
            number_of_donors, type, donor_cluster_ratio, settings_file)
        )
    write_con.commit()

    read_con.close()
    write_con.close()

    #print('ran in', runtime, 'seconds')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='run campaign finance dedupe')
    parser.add_argument('settings_file', help='file to load previously generated setting from')
    parser.add_argument('--type', '-t', default='CORP', help='IND or CORP')
    args = parser.parse_args()

    # ## Setup
    if args.type:
        type = args.type
    else:
        type = 'CORP'

    # ## Run
    run_stats(type,args.settings_file)