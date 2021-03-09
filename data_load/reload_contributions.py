
import dj_database_url
import psycopg2
import argparse

def reload_contributions():
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

    c.execute("INSERT INTO contributions (uuid, donor_id, recipient_id, "
            " trans_id, date, type, amount, contrib_code, receipt_type, "
            " e_year, freport_id ) "
            "SELECT DISTINCT on (uuid) uuid, donor_id, recipient_id, trans_number, date, type, amount, contrib_code, receipt_type, "
            "e_year, freport_id FROM (SELECT concat(filer_id,'-',filing_sched_abbrev,'-',trans_number,'-',replace(replace(sched_date,'/',''),'-','')) as uuid, "
        	"donors.donor_id, filer_id as recipient_id, trans_number, "
            "TO_DATE(TRIM(sched_date), 'YYYY-MM-DD') as date, "
            "filing_sched_abbrev as type, org_amt as amount, "
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
            "FROM (select * from raw_table_state LEFT JOIN contributions as c "
            "ON CONCAT(filer_id,'-',filing_sched_abbrev,'-',trans_number,'-',replace(replace(sched_date,'/',''),'-','')) = c.uuid "
            "WHERE filing_sched_abbrev IN ('A','B','C','D','E') AND c.uuid is null) as o INNER JOIN "
			"(SELECT * FROM donors) as donors ON "
            "((donors.first_name = LOWER(TRIM(o.flng_ent_first_name)) AND "
			 "donors.last_name = LOWER(TRIM(o.flng_ent_last_name))) OR "
            "donors.corp = LOWER(TRIM(o.flng_ent_name))) AND "
            "donors.street = CONCAT(LOWER(TRIM(o.flng_ent_add1)),' ') AND "
            "donors.city = LOWER(TRIM(o.flng_ent_city)) AND "
            "donors.state = LOWER(TRIM(o.flng_ent_state)) AND "
            "donors.zip = LOWER(TRIM(o.flng_ent_zip)) "
            "WHERE source = 'NYSBOE') as l ORDER BY uuid ")
     conn.commit()
    

    c.execute("INSERT INTO contributions (uuid, donor_id, recipient_id, "
            " trans_id, date, type, amount, contrib_code, receipt_type, "
            " e_year, freport_id ) "
            "SELECT DISTINCT on (uuid) uuid, donor_id, recipient_id, trans_number, date, type, amount, contrib_code, receipt_type, "
            "e_year, freport_id FROM (SELECT concat(filer_id,'-',filing_sched_abbrev,'-',trans_number,'-',replace(replace(sched_date,'/',''),'-','')) as uuid, "
        	"donors.donor_id, filer_id as recipient_id, trans_number, "
            "TO_DATE(TRIM(sched_date), 'YYYY-MM-DD') as date, "
            "filing_sched_abbrev as type, org_amt as amount, "
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
            "FROM (select * from raw_table_state LEFT JOIN contributions as c "
            "ON CONCAT(filer_id,'-',filing_sched_abbrev,'-',trans_number,'-',replace(replace(sched_date,'/',''),'-','')) = c.uuid "
            "WHERE filing_sched_abbrev IN ('A','B','C','D','E') AND c.uuid is null) as o INNER JOIN "
			"(SELECT * FROM donors) as donors ON "
            "((donors.first_name = LOWER(TRIM(o.flng_ent_first_name)) AND "
			 "donors.last_name = LOWER(TRIM(o.flng_ent_last_name))) OR "
            "donors.corp = LOWER(TRIM(o.flng_ent_name))) AND "
            "donors.street = CONCAT(LOWER(TRIM(o.flng_ent_add1)),' ') AND "
            "donors.city = LOWER(TRIM(o.flng_ent_city)) AND "
            "donors.state = LOWER(TRIM(o.flng_ent_state)) "
            "WHERE source = 'NYSBOE') as l ORDER BY uuid ")
    conn.commit()
    '''
    c.execute("INSERT INTO contributions (uuid, donor_id, recipient_id, "
            " trans_id, date, type, amount, contrib_code, receipt_type, "
            " e_year, freport_id ) "
            "SELECT DISTINCT on (uuid) uuid, donor_id, recipient_id, trans_number, date, type, amount, contrib_code, receipt_type, "
            "e_year, freport_id FROM (SELECT concat(filer_id,'-',filing_sched_abbrev,'-',trans_number,'-',replace(replace(sched_date,'/',''),'-','')) as uuid, "
        	"donors.donor_id, filer_id as recipient_id, trans_number, "
            "TO_DATE(TRIM(sched_date), 'YYYY-MM-DD') as date, "
            "filing_sched_abbrev as type, org_amt as amount, "
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
            "FROM (select * from raw_table_state LEFT JOIN contributions as c "
            "ON CONCAT(filer_id,'-',filing_sched_abbrev,'-',trans_number,'-',replace(replace(sched_date,'/',''),'-','')) = c.uuid "
            "WHERE filing_sched_abbrev IN ('A','B','C','D','E') AND c.uuid is null) as o INNER JOIN "
			"(SELECT * FROM donors) as donors ON "
            "((donors.first_name = LOWER(TRIM(o.flng_ent_first_name)) AND "
			 "donors.last_name = LOWER(TRIM(o.flng_ent_last_name))) OR "
            "donors.corp = LOWER(TRIM(o.flng_ent_name))) AND "
            "donors.street = CONCAT(LOWER(TRIM(o.flng_ent_add1)),' ') "
            "WHERE source = 'NYSBOE') as l ORDER BY uuid ")

    conn.commit()
    '''
    c.close()
    conn.close()

def finish():
    print("Finished")

if __name__ == '__main__':
    reload_contributions()
    finish()