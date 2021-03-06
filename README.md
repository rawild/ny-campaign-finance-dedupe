# ny-campaign-finance-dedupe
Deduplication Algorithm for NY State Donors

White paper:
https://academicworks.cuny.edu/cgi/viewcontent.cgi?article=5299&context=gc_etds

With the library of dedupe.io: 
https://github.com/dedupeio/dedupe

Documentation: https://docs.dedupe.io/

Use this to match donors from accross campaign donation filings in New York State.
There are 2 sets of donors that are parsed differently. When indicating the type of donor you want to deal with use these codes.
- individual (IND)
- organizational (CORP)



# To use
- install python
- install pip
- use pip to install these libraries:
    - dj_database_url
    - numpy >= 1.9
    - pandas
    - psycopg2
    - psycopg2.extras
    - requests
    - sklearn (only needed if doing random sampling)
    - unidecode
- install postgres sql
- make a database 
- clone the repository
- download data from the New York State Board of Elections: https://www.elections.ny.gov/CFViewReports.html
- run the modules in data_load to prep the data and load it into the database:
    - fix_all_reports.py
    - all_txt_to_csv.py
    - pre_init_db.py (only for first data load into db)
    - init_postgres_db.py
    - clean_donors.py
- run the dedupe extension modules to do the matching
    - campaign_finance_dedupe.py

(Note: the settings directory contains pre-generated settings for the matching or you can generate your own)

# Author
Annalisa Wilde is an MS student at the Graduate Center of the City University of New York in the Data Analysis and Visualization program

Examples of how this data might be used:
- https://who-pays-donors-zfgp8.ondigitalocean.app/
- https://rawild.github.io/InteractiveDataVis-Portfolio/project_1/src/

# Copyright
Copyright (c) 2021 Annalisa Wilde. Released under the GNU General Public License v3.0
