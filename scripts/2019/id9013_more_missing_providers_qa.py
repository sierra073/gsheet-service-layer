from __future__ import division
import psycopg2
import os
import pandas as pd

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")
DATA_PATH = GITHUB + '/Projects/peer_deal_methodology_updates_testing/data/'

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB)
cur = myConnection.cursor()
# get districts (in ESH universe), their line items (SPs and cost) for 2019
os.chdir(GITHUB+'/Projects/sots-isl/scripts/2019/prework_queries')
query = open('id9013_more_missing_providers_qa.sql', "r").read()
cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df_old_deals = pd.DataFrame(rows, columns=names)

df_old_deals['old_peersp_parent_name'] = df_old_deals['old_peersp_parent_name'].apply(lambda s: s.upper())
esh_650_not_matched = pd.read_csv(DATA_PATH + 'processed/esh_sps_not_80_geotel_sps.csv')

print(df_old_deals[df_old_deals.old_peersp_parent_name.isin(esh_650_not_matched['esh_sp'])].district_id.nunique())
