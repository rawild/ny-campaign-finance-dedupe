#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is based off of the setup script for the dedupe-examples postgresql.  It evaluates raw files of all of the campaign
contribution data stored by the NY State Board of Elections and uploads them into a postgres database.

__Note:__ You will need to set the DATABASE_URL with the right connection info for your database

Tables created:
* raw_table - raw import of entire donations file
* donors - all distinct donors based on name and address
* recipients - all distinct campaign contribution recipients - uploaded from entire filers file
* contributions - contribution amounts tied to donor and recipients tables
* processed_donors - donors with some cleaning

sample usage:
python init_postgres_db.py sample_filers.csv sample_filings.csv
"""
import csv
import os
import zipfile

import dj_database_url
import psycopg2
import psycopg2.extras
import unidecode
import requests
import argparse

def processFiles(recipients_file, contributions_file):

    if not os.path.exists(contributions_file):
        print('contributions file not found: %s' % contributions_file)

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

    print('importing raw data from csv...')
    c.execute("DROP TABLE IF EXISTS raw_table")
    c.execute("DROP TABLE IF EXISTS donors")
    c.execute("DROP TABLE IF EXISTS recipients")
    c.execute("DROP TABLE IF EXISTS contributions")
    c.execute("DROP TABLE IF EXISTS processed_donors")
   
    c.execute("CREATE TABLE raw_table "
            "(filer_id VARCHAR(6), freport_id VARCHAR(1), transaction_code VARCHAR(1), "
            " e_year VARCHAR(4), t3_trid VARCHAR(10), date1 VARCHAR(10), "
            " date2 VARCHAR(10), contrib_code VARCHAR(6), contrib_type VARCHAR(1), "
            " corp VARCHAR(50), first_name VARCHAR(50), "
            " mid_init VARCHAR(1), last_name VARCHAR(50), "
            " addr_1 VARCHAR(70), city VARCHAR(50), "
            " state VARCHAR(15), zip VARCHAR(10), "
            " check_no VARCHAR(30), check_date VARCHAR(10), "
            " amount VARCHAR(23), amount2 VARCHAR(23), "
            " description VARCHAR(300), other_recpt_code VARCHAR(50), "
            " purpose_code1 VARCHAR(7), purpose_code2 VARCHAR(40), "
            " explanation VARCHAR(300), xfer_type VARCHAR(20), "
            " chkbox VARCHAR(20), crerec_uid VARCHAR(20), "
            " crerec_date VARCHAR(19))")
    conn.commit()

    with open(contributions_file, 'rU') as csv_file:
        c.copy_expert("COPY raw_table "
                    "(filer_id, freport_id, transaction_code, "
                    " e_year, t3_trid, date1, date2, "
                    " contrib_code, contrib_type, corp, "
                    " first_name, mid_init, last_name, "
                    " addr_1, city, state, zip, check_no,"
                    " check_date, amount, amount2, "
                    " description, other_recpt_code, purpose_code1, "
                    " purpose_code2, explanation, xfer_type, "
                    " chkbox, crerec_uid, "
                    " crerec_date) "
                    "FROM STDIN CSV HEADER", csv_file)
    conn.commit()

    print('creating donors table...')
    c.execute("CREATE TABLE donors "
            "(donor_id SERIAL PRIMARY KEY, "
            " first_name VARCHAR(50), last_name VARCHAR(50), "
            " corp VARCHAR(50), "
            " street VARCHAR(70), type VARCHAR(10), "
            " city VARCHAR(50), state VARCHAR(15), "
            " zip VARCHAR(10))")

    c.execute("INSERT INTO donors "
            "(first_name, last_name, corp, street, "
            " city, state, zip ) "
            "SELECT DISTINCT "
            "LOWER(TRIM(first_name)), LOWER(TRIM(last_name)), "
            "LOWER(TRIM(corp)), "
            "LOWER(TRIM(addr_1)),LOWER(TRIM(city)), "
            "LOWER(TRIM(state)), LOWER(TRIM(zip)) "
            "FROM raw_table "
            "WHERE transaction_code IN ('A','B','C','D','E')")
    conn.commit()

    print('creating indexes on donors table...')
    c.execute("CREATE INDEX donors_donor_info ON donors "
            "(last_name, first_name, corp, street, city, "
            " state, zip)")
    conn.commit()

    if not os.path.exists(recipients_file):
        print('recipients file not found: %s' % recipients_file)

    print('creating recipients table...')
    c.execute("CREATE TABLE recipients "
            "(filer_id VARCHAR(6), name VARCHAR(150), "
            " type VARCHAR(10), status VARCHAR(8), "
            " committee_type VARCHAR(3), "
            " office VARCHAR(3), district INTEGER, "
            " treas_first_name VARCHAR(20), "
            " treas_last_name VARCHAR(40), street VARCHAR(70), "
            " city VARCHAR(30), state VARCHAR(2), zip VARCHAR(11), "
            " candidate_id INTEGER)")

    with open(recipients_file, 'rU') as csv_file:
        c.copy_expert("COPY recipients "
                    "(filer_id, name, type, "
                    " status, committee_type, office, district, "
                    " treas_first_name, treas_last_name, street, "
                    " city, state, zip) "
                    "FROM STDIN CSV HEADER", csv_file)
    conn.commit()

    print('creating contributions table...')
    c.execute("CREATE TABLE contributions "
            "(contribution_id SERIAL PRIMARY KEY, uuid VARCHAR(50), "
            " donor_id INT, recipient_id VARCHAR(6), trans_id VARCHAR(10), "
            " date DATE, type VARCHAR(1), amount VARCHAR(23), "
            " contrib_code VARCHAR(6), "
            " receipt_type VARCHAR(25), "
            " e_year VARCHAR(10), freport_id VARCHAR(1))")


    c.execute("INSERT INTO contributions (uuid, donor_id, recipient_id, "
            " trans_id, date, type, amount, contrib_code, receipt_type, "
            " e_year, freport_id ) "
            "SELECT concat(filer_id,'-',transaction_code,'-',t3_trid,'-',replace(date1,'/','')) as uuid, donors.donor_id, filer_id as recipient_id, t3_trid, "
            " TO_DATE(TRIM(date1), 'MM/DD/YY'), "
            " transaction_code as type, amount, "
            " contrib_code,  "
            " CASE WHEN (other_recpt_code IS NULL AND contrib_type IS NOT NULL) "
            "      THEN LOWER(CONCAT('C',contrib_type)) "
            "      WHEN (other_recpt_code IS NOT NULL AND contrib_type IS NULL) "
            "      THEN LOWER(CONCAT('R',other_recpt_code)) "
            "      ELSE NULL "
            " END AS receipt_type, "
            " e_year, freport_id "
            "FROM raw_table JOIN donors ON "
            "((donors.first_name = LOWER(TRIM(raw_table.first_name)) AND "
            "donors.last_name = LOWER(TRIM(raw_table.last_name))) OR "
            "donors.corp = LOWER(TRIM(raw_table.corp))) AND "
            "donors.street = LOWER(TRIM(raw_table.addr_1)) AND "
            "donors.city = LOWER(TRIM(raw_table.city)) AND "
            "donors.state = LOWER(TRIM(raw_table.state)) AND "
            "donors.zip = LOWER(TRIM(raw_table.zip)) " 
            "WHERE transaction_code IN ('A','B','C','D','E')")
    conn.commit()

    print('creating indexes on contributions...')
    c.execute("CREATE INDEX donor_idx ON contributions (donor_id)")
    c.execute("CREATE INDEX recipient_idx ON contributions (recipient_id)")
    conn.commit()

    print('nullifying empty strings in donors...')
    c.execute(
        "UPDATE donors "
        "SET "
        "first_name = CASE first_name WHEN '' THEN NULL ELSE first_name END, "
        "last_name = CASE last_name WHEN '' THEN NULL ELSE last_name END, "
        "corp = CASE corp WHEN '' THEN NULL ELSE corp END, "
        "street = CASE street WHEN '' THEN NULL ELSE street END, "
        "city = CASE city WHEN '' THEN NULL ELSE city END, "
        "state = CASE state WHEN '' THEN NULL ELSE state END, "
        "zip = CASE zip WHEN '' THEN NULL ELSE zip END"
    )
    conn.commit()

    print('creating processed_donors...')
    c.execute("CREATE TABLE processed_donors AS "
            "(SELECT donor_id, "
            " LOWER(city) AS city, "
            " CASE WHEN (first_name IS NULL AND last_name IS NULL) "
            "      THEN LOWER(corp) "
            "      ELSE LOWER(CONCAT_WS(' ', first_name, last_name)) "
            " END AS name, " 
            " LOWER(zip) AS zip, "
            " LOWER(state) AS state, "
            " LOWER(street) AS street, " 
            " CAST((first_name IS NOT NULL) AS INTEGER) AS person "
            " FROM donors)")
    c.execute("CREATE INDEX processed_donor_idx ON processed_donors (donor_id)")
    conn.commit()

    c.close()
    conn.close()

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='init_postgres_db')
    parser.add_argument('recipients_file', help='filers from boe')
    parser.add_argument('contributions_file', help='filings from boe')
    args=parser.parse_args()
    processFiles(args.recipients_file,args.contributions_file)
    finish()