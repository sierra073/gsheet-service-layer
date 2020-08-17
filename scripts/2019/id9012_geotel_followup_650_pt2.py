# 1) What percent of districts are served by these 650 providers?
# 2) How much erate $ do they get as a percent of total IA erate $?

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
os.chdir(GITHUB+'/''scripts/2019/prework_queries')
query = open('id9012_geotel_followup_650_pt2.sql', "r").read()
cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df_bw_cost = pd.DataFrame(rows, columns=names)

df_bw_cost['parent_name'] = df_bw_cost['parent_name'].fillna('').apply(lambda s: s.upper())
esh_650_not_matched = pd.read_csv(DATA_PATH + 'processed/esh_sps_not_80_geotel_sps.csv')
# esh_650_not_matched = esh_650_not_matched[~esh_650_not_matched.esh_sp.isin(['FRONTIER', 'ZITO MEDIA COMMUNICATIONS'])]

# 1)
print(df_bw_cost[df_bw_cost.parent_name.isin(esh_650_not_matched['esh_sp'])].district_id.nunique())
pct_districts_served_by_650 = df_bw_cost[df_bw_cost.parent_name.isin(esh_650_not_matched['esh_sp'])].district_id.nunique()/df_bw_cost.district_id.nunique()
print(pct_districts_served_by_650)  # 63.7%, 62.4% excluding 'major' Frontier and match error Zito Media Communications

# 2)
total_ia_annual_cost_erate = df_bw_cost['ia_annual_cost_erate'].sum()  # 607M
pct_total_ia_cost_erate_650 = df_bw_cost[df_bw_cost.parent_name.isin(esh_650_not_matched['esh_sp'])]['ia_annual_cost_erate'].sum()/total_ia_annual_cost_erate
print(pct_total_ia_cost_erate_650)  # 53%, 52% excluding 'major' Frontier and match error Zito Media Communications

results = pd.DataFrame({
          'Pct of districts served by 650': pct_districts_served_by_650,
          'Pct total IA erate $ from 650': pct_total_ia_cost_erate_650
          }, index=[0])

# save csv
os.chdir(GITHUB + '/''data')
results.to_csv("id9012_geotel_followup_650_pt2.csv", index=False)
