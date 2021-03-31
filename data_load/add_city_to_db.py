"""
This is based off of the setup script for the dedupe-examples postgresql.  It evaluates raw files of all of the campaign
contribution data stored by the NY City Board and uploads them into a postgres database.

__Note:__ You will need to set the DATABASE_URL with the right connection info for your database

Tables created:
* raw_table_city - raw import of entire donations file
* recipients_city - all distinct campaign contribution recipients - uploaded from the raw file
* contributions_city - all distinct campaign contributions - uploaded from the raw file
Updated tables:
* donors - all distinct donors based on name and address
* processed_donors - donors with some cleaning

sample usage:
python3 add_city_to_db.py ../../NYCCFB/city_filings.csv

Source control: First created March 19th, 2021
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

def processFiles(city_contrib_file):

    if not os.path.exists(city_contrib_file):
        print('state contributions file not found: %s' % city_contrib_file)

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

    c.execute("DROP TABLE IF EXISTS raw_table_city")
    c.execute("DROP TABLE IF EXISTS recipients_city")
    c.execute("DROP TABLE IF EXISTS contributions_city")

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
    c.execute("DELETE FROM donors WHERE source = 'NYCCFB'")
    c.execute("INSERT INTO donors "
            "(first_name, middle_name, last_name, corp, street, "
            " city, state, zip, type, source) "
            "SELECT DISTINCT "
            "CASE LOWER(TRIM(donorfirst)) WHEN '' THEN NULL ELSE LOWER(TRIM(donorfirst)) END, "
            "CASE LOWER(TRIM(donormi)) WHEN '' THEN NULL ELSE LOWER(TRIM(donormi)) END, "
            "CASE LOWER(TRIM(donorlast)) WHEN '' THEN NULL ELSE LOWER(TRIM(donorlast)) END, "
            "CASE LOWER(TRIM(donorcorp)) WHEN '' THEN NULL ELSE LOWER(TRIM(donorcorp)) END, "
            "CASE LOWER(TRIM(CONCAT(TRIM(strno),' ',TRIM(strname),' ',TRIM(apartment)))) WHEN '' THEN NULL ELSE LOWER(TRIM(CONCAT(TRIM(strno),' ',TRIM(strname),' ',TRIM(apartment)))) END,"
            "CASE LOWER(TRIM(city)) WHEN '' THEN NULL ELSE LOWER(TRIM(city)) END, "
            "CASE LOWER(TRIM(state)) WHEN '' THEN NULL ELSE LOWER(TRIM(state)) END, "
            "CASE LOWER(TRIM(zip)) WHEN '' THEN NULL ELSE LOWER(TRIM(zip)) END, "
            "CASE UPPER(TRIM(c_code)) WHEN '' THEN 'UNK' "
            "WHEN 'PCOM' THEN 'PAC' "
            "WHEN 'PART' THEN 'PART' "
            "WHEN 'UNKN' THEN 'UNK' "
            "WHEN 'LLC' THEN 'CORP' "
            "WHEN 'FAM' THEN 'FAM' "
            "WHEN 'SPO' THEN 'FAM' "
            "WHEN 'PCOMP' THEN 'PAC' "
            "WHEN 'CAN' THEN 'FAM' "
            "WHEN 'PCOMC' THEN 'COM' "
            "WHEN 'ORG' THEN 'CORP' "
            "WHEN 'PCOMZ' THEN 'PAC' "
            "WHEN 'CORP' THEN 'CORP' "
            "WHEN 'IND' THEN 'IND' "
            "WHEN 'EMPO' THEN 'CORP' "
            "WHEN 'OTHR' THEN 'OTHER' "
            "ELSE 'UNK' END, 'NYCCFB' "
            "FROM raw_table_city")
    conn.commit()

    print('nullifying empty strings in donors...')
    c.execute(
        "UPDATE donors "
        "SET "
        "first_name = CASE first_name WHEN '' THEN NULL ELSE first_name END, "
        "middle_name = CASE middle_name WHEN '' THEN NULL ELSE middle_name END, "
        "last_name = CASE last_name WHEN '' THEN NULL ELSE last_name END, "
        "corp = CASE corp WHEN '' THEN NULL ELSE corp END, "
        "street = CASE street WHEN '' THEN NULL ELSE street END, "
        "city = CASE city WHEN '' THEN NULL ELSE city END, "
        "state = CASE state WHEN '' THEN NULL ELSE state END, "
        "zip = CASE zip WHEN '' THEN NULL ELSE zip END"
    )
    conn.commit()

    print('creating city recipients table...')
    c.execute("CREATE TABLE recipients_city "
            "(filer_id VARCHAR(6), name VARCHAR(150),"
            " officecd VARCHAR(10), committee VARCHAR(100), "
            " status VARCHAR(8), "
            " committee_type VARCHAR(200), "
            " municipality VARCHAR(100),"
            " candidate_id INTEGER)")
    
    print('adding city to the city recipients table...')

    c.execute("INSERT INTO recipients_city "
            "(filer_id, name, committee_type, officecd, municipality) "
            "SELECT DISTINCT "
            "LOWER(CONCAT(TRIM(recipid))), "
            "LOWER(CONCAT(TRIM(candfirst),' ',TRIM(candmi),' ',TRIM(candlast))), "
            "LOWER(TRIM(officecd)), LOWER(TRIM(committee)), 'new york city' "
            "FROM raw_table_city")
    conn.commit()

    print('creating city contributions table...')
    c.execute("CREATE TABLE contributions_city "
            "(contribution_id SERIAL PRIMARY KEY, uuid VARCHAR(100), "
            " donor_id INT, recipient_id VARCHAR(6), trans_id VARCHAR(100), "
            " date DATE, type VARCHAR(10), amount VARCHAR(30), "
            " contrib_code VARCHAR(6), "
            " receipt_type VARCHAR(25), "
            " purpose_code VARCHAR(80), "
            " e_year VARCHAR(10), freport_id VARCHAR(10))")

    print('adding city to the contributions table...')
    c.execute("INSERT INTO contributions_city (uuid, donor_id, recipient_id, "
            " trans_id, date, contrib_code, amount, type, receipt_type, "
            " e_year, freport_id ) "
            "SELECT concat(recipid,'-',filing,'-',refno,'-',replace(replace(date,'/',''),'-','')) as uuid, "
            "donors.donor_id, recipid AS recipient_id, "
            " refno AS trans_id, "
            " TO_DATE(TRIM(date), 'MM/DD/YYYY') AS date, "
            "CASE UPPER(TRIM(c_code)) WHEN '' THEN 'UNK' "
            "WHEN 'PCOM' THEN 'PAC' "
            "WHEN 'PART' THEN 'PART' "
            "WHEN 'UNKN' THEN 'UNK' "
            "WHEN 'LLC' THEN 'CORP' "
            "WHEN 'FAM' THEN 'FAM' "
            "WHEN 'SPO' THEN 'FAM' "
            "WHEN 'PCOMP' THEN 'PAC' "
            "WHEN 'CAN' THEN 'FAM' "
            "WHEN 'PCOMC' THEN 'COM' "
            "WHEN 'ORG' THEN 'CORP' "
            "WHEN 'PCOMZ' THEN 'PAC' "
            "WHEN 'CORP' THEN 'CORP' "
            "WHEN 'IND' THEN 'IND' "
            "WHEN 'EMPO' THEN 'CORP' "
            "WHEN 'OTHR' THEN 'OTHER' "
            "ELSE 'UNK' END as contrib_code, amnt AS amount, "
            " schedule AS type,  "
            " 'NYCCFB' AS receipt_type, "
            " election AS e_year, filing AS freport_id "
            "FROM raw_table_city JOIN donors ON "
            "CASE WHEN donors.first_name IS NULL THEN '' ELSE donors.first_name END = CASE WHEN LOWER(TRIM(raw_table_city.donorfirst)) IS NULL THEN '' ELSE LOWER(TRIM(raw_table_city.donorfirst)) END AND "
            "CASE WHEN donors.middle_name IS NULL THEN '' ELSE donors.middle_name END = CASE WHEN LOWER(TRIM(raw_table_city.donormi)) IS NULL THEN '' ELSE LOWER(TRIM(raw_table_city.donormi)) END AND "
            "CASE WHEN donors.last_name IS NULL THEN '' ELSE donors.last_name END = CASE WHEN LOWER(TRIM(raw_table_city.donorlast)) IS NULL THEN '' ELSE LOWER(TRIM(raw_table_city.donorlast)) END AND "
            "CASE WHEN donors.corp IS NULL THEN '' ELSE donors.corp END = CASE WHEN LOWER(TRIM(raw_table_city.donorcorp)) IS NULL THEN '' ELSE LOWER(TRIM(raw_table_city.donorcorp)) END AND "
            "CASE WHEN donors.street IS NULL THEN '' ELSE donors.street END = CASE WHEN LOWER(TRIM(CONCAT(TRIM(raw_table_city.strno),' ',TRIM(raw_table_city.strname),' ',TRIM(raw_table_city.apartment)))) IS NULL THEN '' ELSE LOWER(TRIM(CONCAT(TRIM(raw_table_city.strno),' ',TRIM(raw_table_city.strname),' ',TRIM(raw_table_city.apartment)))) END AND "
            "CASE WHEN donors.city IS NULL THEN '' ELSE donors.city END = CASE WHEN LOWER(TRIM(raw_table_city.city)) IS NULL THEN '' ELSE LOWER(TRIM(raw_table_city.city)) END AND "
            "CASE WHEN donors.state IS NULL THEN '' ELSE donors.state END = CASE WHEN LOWER(TRIM(raw_table_city.state)) IS NULL THEN '' ELSE LOWER(TRIM(raw_table_city.state)) END AND "
            "CASE WHEN donors.zip IS NULL THEN '' ELSE donors.zip END = CASE WHEN LOWER(TRIM(raw_table_city.zip)) IS NULL THEN '' ELSE LOWER(TRIM(raw_table_city.zip)) END AND "
            "donors.source = 'NYCCFB' AND "
            "donors.type = CASE UPPER(TRIM(c_code)) WHEN '' THEN 'UNK' "
            "WHEN 'PCOM' THEN 'PAC' "
            "WHEN 'PART' THEN 'PART' "
            "WHEN 'UNKN' THEN 'UNK' "
            "WHEN 'LLC' THEN 'CORP' "
            "WHEN 'FAM' THEN 'FAM' "
            "WHEN 'SPO' THEN 'FAM' "
            "WHEN 'PCOMP' THEN 'PAC' "
            "WHEN 'CAN' THEN 'FAM' "
            "WHEN 'PCOMC' THEN 'COM' "
            "WHEN 'ORG' THEN 'CORP' "
            "WHEN 'PCOMZ' THEN 'PAC' "
            "WHEN 'CORP' THEN 'CORP' "
            "WHEN 'IND' THEN 'IND' "
            "WHEN 'EMPO' THEN 'CORP' "
            "WHEN 'OTHR' THEN 'OTHER' "
            "ELSE 'UNK' END")
    conn.commit()

    print('dropping processed_donors')
    c.execute("DROP TABLE IF EXISTS processed_donors")
   
    print('re-creating processed_donors...')
    c.execute("CREATE TABLE processed_donors AS "
            "(SELECT donor_id, "
            " LOWER(city) AS city, "
            " CASE WHEN (first_name IS NULL AND last_name IS NULL) "
            "      THEN LOWER(corp) "
            "      ELSE LOWER(CONCAT_WS(' ', first_name, middle_name, last_name)) "
            " END AS name, " 
            " LOWER(zip) AS zip, "
            " LOWER(state) AS state, "
            " LOWER(street) AS street, " 
            " CAST((CASE type WHEN 'IND' THEN '1' WHEN 'FAM' THEN '1' ELSE '0' END) AS INTEGER) AS person "
            " FROM donors)")
    c.execute("CREATE INDEX processed_donor_idx ON processed_donors (donor_id)")
    conn.commit()

    c.close()
    conn.close()

def finish():
    print("Finished")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='add_city_to_db')
    parser.add_argument('city_contrib_file', help='filings from city cfb')
    args=parser.parse_args()
    processFiles(args.city_contrib_file)
    finish()