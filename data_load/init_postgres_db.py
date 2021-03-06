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

Source control: Updated March 05 2021 to reflect updated data structure from the NYSBOE (Jan 22, 2021)
Also on March 05 2021 updated to include city data
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

def processFiles(state_recip_file, state_contrib_file, city_contrib_file):

    if not os.path.exists(state_contrib_file):
        print('state contributions file not found: %s' % state_contrib_file)

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

    
    c.execute("DROP TABLE IF EXISTS raw_table_state")
    c.execute("DROP TABLE IF EXISTS raw_table_city")
    c.execute("DROP TABLE IF EXISTS donors")
    c.execute("DROP TABLE IF EXISTS recipients")
    c.execute("DROP TABLE IF EXISTS contributions")
    c.execute("DROP TABLE IF EXISTS processed_donors")
   
    print('importing raw data from state csv...')
    c.execute("CREATE TABLE raw_table_state "
            "(filer_id VARCHAR(6), filer_id_prev VARCHAR(6), " 
            " election_year VARCHAR(4), election_type VARCHAR(100), "
            " county_desc VARCHAR(255), filing_abbrev VARCHAR(1), filing_desc VARCHAR(80), "
            " r_amend VARCHAR(1), filing_cat_desc VARCHAR(80), filing_sched_abbrev VARCHAR(1), "
            " filing_sched_desc VARCHAR(80), loan_lib_number VARCHAR(100), trans_number VARCHAR(100), "
            " trans_mapping VARCHAR(100), sched_date VARCHAR(10), org_date VARCHAR(10), "
            " cntrbr_type_desc VARCHAR(80), cntrbn_type_desc VARCHAR(80), transfer_type_desc VARCHAR(200), "
            " receipt_type_desc VARCHAR(80), receipt_type_code VARCHAR(80), purpose_code_desc VARCHAR(80), "
            " r_subscontractor VARCHAR(1), flng_ent_name VARCHAR(250), flng_ent_first_name VARCHAR(100), "
            " flng_ent_middle_name VARCHAR(50), flng_ent_last_name VARCHAR(100), flng_ent_add1 VARCHAR(100), "
            " flng_ent_add2 VARCHAR(30), "
            " flng_ent_city VARCHAR(50), flng_ent_state VARCHAR(20), flng_ent_zip VARCHAR(30), "
            " flng_ent_country VARCHAR(30), payment_type_desc VARCHAR(80), pay_number VARCHAR(30), "
            " owed_amt VARCHAR(20), org_amt VARCHAR(50), loan_other_desc VARCHAR(80), "
            " trans_explntn VARCHAR(300), r_itemized VARCHAR(4), r_liability VARCHAR(1), "
            " election_year_2 VARCHAR(4), office_desc VARCHAR(100), district VARCHAR(40), "
            " dist_off_cand_bal_prop VARCHAR(500))")
    conn.commit()

    with open(state_contrib_file, 'r+') as csv_file:
        c.copy_expert("COPY raw_table_state "
                    "(filer_id, filer_id_prev, " 
                    " election_year, election_type, "
                    " county_desc, filing_abbrev, filing_desc, "
                    " r_amend, filing_cat_desc, filing_sched_abbrev, "
                    " filing_sched_desc, loan_lib_number, trans_number, "
                    " trans_mapping, sched_date, org_date, "
                    " cntrbr_type_desc, cntrbn_type_desc, transfer_type_desc, "
                    " receipt_type_desc, receipt_type_code, purpose_code_desc, "
                    " r_subscontractor, flng_ent_name, flng_ent_first_name, "
                    " flng_ent_middle_name, flng_ent_last_name, flng_ent_add1, "
                    " flng_ent_add2, "
                    " flng_ent_city, flng_ent_state, flng_ent_zip, "
                    " flng_ent_country, payment_type_desc, pay_number, "
                    " owed_amt, org_amt, loan_other_desc, "
                    " trans_explntn, r_itemized, r_liability, "
                    " election_year_2, office_desc, district, "
                    " dist_off_cand_bal_prop) "
                    "FROM STDIN CSV HEADER", csv_file)
    conn.commit()

    print('creating donors table...')
    c.execute("CREATE TABLE donors "
            "(donor_id SERIAL PRIMARY KEY, "
            " first_name VARCHAR(50), middle_name VARCHAR(30), last_name VARCHAR(50), "
            " corp VARCHAR(50), "
            " street VARCHAR(70), "
            " city VARCHAR(50), state VARCHAR(15), "
            " zip VARCHAR(20), type VARCHAR(10), source VARCHAR(6))")

    c.execute("INSERT INTO donors "
            "(first_name, middle_name, last_name, corp, street, "
            " city, state, zip, type, source) "
            "SELECT DISTINCT "
            "LOWER(TRIM(flng_ent_first_name)), LOWER(TRIM(flng_ent_middle_name)), LOWER(TRIM(flng_ent_last_name)), "
            "LOWER(TRIM(flng_ent_name)), "
            "LOWER(CONCAT(TRIM(flng_ent_add1),' ',TRIM(flng_ent_add2))),LOWER(TRIM(flng_ent_city)), "
            "LOWER(TRIM(flng_ent_state)), LOWER(TRIM(flng_ent_zip)), "
            "CASE WHEN cntrbr_type_desc = 'Candidate/Canditate Spouse' "
            "THEN 'CAN' "
            "WHEN cntrbr_type_desc = 'Individual' "
            "THEN 'IND' "
            "WHEN cntrbr_type_desc = 'Unitemized' "
            "THEN 'UNITEM' "
            "WHEN cntrbr_type_desc = 'Partnership, including LLPs' "
            "THEN 'PART' "
            "WHEN cntrbr_type_desc = 'Candidate Family Member' "
            "THEN 'FAM' "
            "WHEN cntrbr_type_desc = 'Political Committee' "
            "THEN 'PAC' "
            "WHEN cntrbr_type_desc = 'Political Action Committee (PAC)' "
            "THEN 'PAC' "
            "WHEN cntrbr_type_desc = 'Committee' "
            "THEN 'COM' "
            "WHEN cntrbr_type_desc = 'Other' "
            "THEN 'OTHER' "
            "WHEN cntrbr_type_desc = 'Sole Proprietorship' "
            "THEN 'CORP' "
            "WHEN cntrbr_type_desc = 'Corporation' "
            "THEN 'CORP' "
            "WHEN cntrbr_type_desc IS NULL "
            "THEN 'CORP' "
            "ELSE 'UNK' "
            "END AS type, 'NYSBOE' as source "
            "FROM raw_table_state "
            "WHERE filing_sched_abbrev IN ('A','B','C','D','E')")
    conn.commit()

    print('importing raw data from city csv...')
    c.execute("CREATE TABLE raw_table_city "
            "(election VARCHAR(4), officecd VARCHAR(5), recipid VARCHAR(5), canclass VARCHAR(3), "
            "candfirst VARCHAR(100), candlast VARCHAR(100), candmi VARCHAR(1), "
            "committee VARCHAR(1), filing VARCHAR(4), schedule VARCHAR(8), "
            "pageno VARCHAR(3), sequenceno VARCHAR(3), refno VARCHAR(20), date VARCHAR(15), "
            "refunddate VARCHAR(15), name VARCHAR(300), c_code VARCHAR(10), "
            "donorcorp VARCHAR(300), donorfirst VARCHAR(250), donorlast VARCHAR(250), donormi VARCHAR(20), "
            "strno VARCHAR(8), "
            "strname VARCHAR(70), apartment VARCHAR(20), boroughcd VARCHAR(1), city VARCHAR(50), "
            "state VARCHAR(15), zip VARCHAR(12), occupation VARCHAR(70), empname VARCHAR(200), "
            "empstrno VARCHAR(15), empstrname VARCHAR(80), empcity VARCHAR(50), empstate VARCHAR(15), "
            "amnt VARCHAR(15), matchamnt VARCHAR(15), prevamnt VARCHAR(15), pay_method VARCHAR(10), "
            "intermno VARCHAR(15), intermname VARCHAR(100), intstrno VARCHAR(25), intstrnm VARCHAR(80), "
            "intaptno VARCHAR(20), intcity VARCHAR(50), intst VARCHAR(2), intzip VARCHAR(10), "
            "intempname VARCHAR(100), intempstno VARCHAR(8), intempstnm VARCHAR(80), intempcity VARCHAR(50), "
            "intempst VARCHAR(2), intoccupa VARCHAR(70), purposecd VARCHAR(10), exemptcd VARCHAR(10), "
            "adjtypecd VARCHAR(10), rr_ind VARCHAR(10), seg_ind VARCHAR(10), int_c_code VARCHAR(10))")
    conn.commit()

    if not os.path.exists(city_contrib_file):
        print('city contributions file not found: %s' % city_contrib_file)

    with open(city_contrib_file, 'r+') as csv_file:
        c.copy_expert("COPY raw_table_city "
                    "(election, officecd, recipid, canclass, candfirst, "
                    "candlast, candmi, committee, "
                    "filing, schedule, pageno, sequenceno, refno, date, refunddate, "
                    "name, c_code, donorcorp, donorfirst, donorlast, donormi, "
                    "strno, strname, apartment, boroughcd, city, state, "
                    "zip, occupation, empname, empstrno, empstrname, empcity, empstate, "
                    "amnt, matchamnt, prevamnt, pay_method, intermno, intermname, "
                    "intstrno, intstrnm, intaptno, intcity, intst, intzip, intempname, "
                    "intempstno, intempstnm, intempcity, intempst, intoccupa, purposecd, "
                    "exemptcd, adjtypecd, rr_ind, seg_ind, int_c_code) "
                    "FROM STDIN CSV HEADER", csv_file)
    conn.commit()

    print('adding to donors table...')

    c.execute("INSERT INTO donors "
            "(first_name, middle_name, last_name, corp, street, "
            " city, state, zip, type, source) "
            "SELECT DISTINCT "
            "LOWER(TRIM(donorfirst)), LOWER(TRIM(donormi)), LOWER(TRIM(donorlast)), "
            "LOWER(TRIM(name)), "
            "LOWER(CONCAT(TRIM(strno),' ',TRIM(strname),' ',TRIM(apartment))),LOWER(TRIM(city)), "
            "LOWER(TRIM(state)), LOWER(TRIM(zip)), "
            "LOWER(TRIM(c_code)), 'NYCCFB' "
            "FROM raw_table_city")
    conn.commit()


    print('creating indexes on donors table...')
    c.execute("CREATE INDEX donors_donor_info ON donors "
            "(last_name, first_name, corp, street, city, "
            " state, zip, type)")
    conn.commit()

    if not os.path.exists(state_recip_file):
        print('state recipients file not found: %s' % state_recip_file)

    print('creating recipients table...')
    c.execute("CREATE TABLE recipients "
            "(filer_id VARCHAR(6), name VARCHAR(150), "
            " compliance_type_desc VARCHAR(10), filer_type_desc VARCHAR(100), "
            " status VARCHAR(8), "
            " committee_type VARCHAR(200), "
            " office VARCHAR(50), district VARCHAR(15), county VARCHAR(255), "
            " municipality VARCHAR(100), treas_first_name VARCHAR(40), "
            " treas_middle_name VARCHAR(100), treas_last_name VARCHAR(40), street VARCHAR(70), "
            " city VARCHAR(30), state VARCHAR(15), zip VARCHAR(11), "
            " candidate_id INTEGER)")

    with open(state_recip_file, 'r+') as csv_file:
        c.copy_expert("COPY recipients "
                    "(filer_id, name, compliance_type_desc, filer_type_desc, "
                    " status, committee_type, office, district, county, municipality, "
                    " treas_first_name, treas_middle_name, treas_last_name, street, "
                    " city, state, zip) "
                    "FROM STDIN CSV HEADER", csv_file)
    conn.commit()

    print('adding city to the recipients table...')

    c.execute("INSERT INTO recipients "
            "(filer_id, name, committee_type, office, municipality) "
            "SELECT DISTINCT "
            "LOWER(CONCAT('c_',TRIM(recipid))), "
            "LOWER(CONCAT(TRIM(candfirst),' ',TRIM(candmi),' ',TRIM(candlast))), "
            "LOWER(TRIM(committee)), LOWER(TRIM(officecd)), 'new york city' "
            "FROM raw_table_city")
    conn.commit()


    print('creating contributions table...')
    c.execute("CREATE TABLE contributions "
            "(contribution_id SERIAL PRIMARY KEY, uuid VARCHAR(100), "
            " donor_id INT, recipient_id VARCHAR(6), trans_id VARCHAR(100), "
            " date DATE, type VARCHAR(1), amount VARCHAR(23), "
            " contrib_code VARCHAR(6), "
            " receipt_type VARCHAR(25), "
            " e_year VARCHAR(10), freport_id VARCHAR(10))")


    c.execute("INSERT INTO contributions (uuid, donor_id, recipient_id, "
            " trans_id, date, type, amount, contrib_code, receipt_type, "
            " e_year, freport_id ) "
            "SELECT concat(filer_id,'-',filing_sched_abbrev,'-',trans_number,'-',replace(replace(sched_date,'/',''),'-','')) as uuid, donors.donor_id, "
            " filer_id as recipient_id, trans_number, "
            " TO_DATE(TRIM(sched_date), 'YYYY-MM-DD'), "
            " filing_sched_abbrev as type, org_amt as amount, "
            "CASE WHEN cntrbr_type_desc = 'Candidate/Canditate Spouse' "
            "THEN 'CAN' "
            "WHEN cntrbr_type_desc = 'Individual' "
            "THEN 'IND' "
            "WHEN cntrbr_type_desc = 'Unitemized' "
            "THEN 'UNITEM' "
            "WHEN cntrbr_type_desc = 'Partnership, including LLPs' "
            "THEN 'PART' "
            "WHEN cntrbr_type_desc = 'Candidate Family Member' "
            "THEN 'FAM' "
            "WHEN cntrbr_type_desc = 'Political Committee' "
            "THEN 'PAC' "
            "WHEN cntrbr_type_desc = 'Political Action Committee (PAC)' "
            "THEN 'PAC' "
            "WHEN cntrbr_type_desc = 'Committee' "
            "THEN 'COM' "
            "WHEN cntrbr_type_desc = 'Other' "
            "THEN 'OTHER' "
            "WHEN cntrbr_type_desc = 'Sole Proprietorship' "
            "THEN 'CORP' "
            "WHEN cntrbr_type_desc = 'Corporation' "
            "THEN 'CORP' "
            "WHEN cntrbr_type_desc IS NULL "
            "THEN 'CORP' "
            "ELSE 'UNK' "
            "END AS contrib_code, 'NYSBOE' as receipt_type, "
            " election_year as e_year, filing_abbrev as freport_id "
            "FROM raw_table_state JOIN donors ON "
            "((donors.first_name = LOWER(TRIM(raw_table_state.flng_ent_first_name)) AND "
            "donors.middle_name = LOWER(TRIM(raw_table_state.flng_ent_middle_name)) AND "
            "donors.last_name = LOWER(TRIM(raw_table_state.flng_ent_last_name))) OR "
            "donors.corp = LOWER(TRIM(raw_table_state.flng_ent_name))) AND "
            "donors.street = LOWER(CONCAT(TRIM(raw_table_state.flng_ent_add1),' ',TRIM(raw_table_state.flng_ent_add2))) AND "
            "donors.city = LOWER(TRIM(raw_table_state.flng_ent_city)) AND "
            "donors.state = LOWER(TRIM(raw_table_state.flng_ent_state)) AND "
            "donors.zip = LOWER(TRIM(raw_table_state.flng_ent_zip)) " 
            "WHERE filing_sched_abbrev IN ('A','B','C','D','E')")
    conn.commit()

    print('adding city to the contributions table...')
    c.execute("INSERT INTO contributions (uuid, donor_id, recipient_id, "
            " trans_id, date, type, amount, contrib_code, receipt_type, "
            " e_year, freport_id ) "
            "SELECT concat(recipid,'-',filing,'-',refno,'-',replace(replace(date,'/',''),'-','')) as uuid, donors.donor_id, recipid AS recipient_id, "
            " refno AS trans_id, "
            " TO_DATE(TRIM(date), 'MM/DD/YY') AS date, "
            " CASE WHEN c_code IN ('IND','PART') "
            " THEN 'A' "
            " WHEN c_code = 'CORP' "
            " THEN 'B' "
            " ELSE 'C' END as type, amnt AS amount, "
            " c_code AS contrib_code,  "
            " 'NYCCFB' AS receipt_type, "
            " election AS e_year, filing AS freport_id "
            "FROM raw_table_city JOIN donors ON "
            "((donors.first_name = LOWER(TRIM(raw_table_city.donorfirst)) AND "
            "donors.middle_name = LOWER(TRIM(raw_table_city.donormi)) AND "
            "donors.last_name = LOWER(TRIM(raw_table_city.donorlast))) OR "
            "donors.corp = LOWER(TRIM(raw_table_city.name))) AND "
            "donors.street = LOWER(CONCAT(TRIM(raw_table_city.strno),' ',TRIM(raw_table_city.strname),' ',TRIM(raw_table_city.apartment))) AND "
            "donors.city = LOWER(TRIM(raw_table_city.city)) AND "
            "donors.state = LOWER(TRIM(raw_table_city.state)) AND "
            "donors.zip = LOWER(TRIM(raw_table_city.zip)) AND "
            "donors.source = 'NYCCFB'")
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
    parser.add_argument('state_recip_file', help='filers from state boe')
    parser.add_argument('state_contrib_file', help='filings from state boe')
    parser.add_argument('city_contrib_file', help='filings from city boe')
    args=parser.parse_args()
    processFiles(args.state_recip_file,args.state_contrib_file,args.city_contrib_file)
    finish()