'''
PURPOSE: Sots 2019 follow up
for a specific set of 65 districts, when was the last time they upgraded if ever?
'''

from __future__ import division
import os
import psycopg2
import pandas as pd

# connection credentials
HOST = os.environ.get("HOST_DAR_PROD")
USER = os.environ.get("USER_DAR_PROD")
PASSWORD = os.environ.get("PASSWORD_DAR_PROD")
DB = os.environ.get("DB_DAR_PROD")
GITHUB = os.environ.get("GITHUB")

# create cursor connection
myConnection = psycopg2.connect( host=HOST, user=USER, password=PASSWORD, database=DB)
cur = myConnection.cursor()

def get_districts():
    query = "select * from ps.districts"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_d = pd.DataFrame(rows, columns=names)
    return df_d

def get_districts_fit_for_analysis():
    query = "select * from ps.districts_fit_for_analysis"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_fit = pd.DataFrame(rows, columns=names)
    return df_fit

def get_districts_upgrades():
    query = "select * from ps.districts_upgrades"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_up = pd.DataFrame(rows, columns=names)
    return df_up


# Select subset of columns
df_d_cols = ['district_id', 'funding_year', 'name', 'district_type', 'state_code','in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_up_cols = ['district_id', 'funding_year', 'upgrade_indicator']

# get tables from dar prod database
df_d = get_districts()
df_fit = get_districts_fit_for_analysis()
df_up = get_districts_upgrades()

# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_up[df_up_cols],
                         on=merge_cols)


# filter the dataframe
df_filtered_ia = df[(df.in_universe==True) &
                 (df.district_type=='Traditional') &
                 (df.fit_for_ia==True)]

# load 65 districts from justine's list
os.chdir(GITHUB + '/''scripts/2019/prework_queries')
df_65 = pd.read_csv("SAT shares with Evan 8.20 - 65 districts make up 2M.csv")

# find year district upgraded
df_results = pd.DataFrame()

for i, d_id in enumerate(df_65['district id'].values):
    df_temp = df_filtered_ia[(df_filtered_ia.district_id == d_id) & (df_filtered_ia.upgrade_indicator == True)]
    df_results.loc[i, 'district_id'] = round(int(d_id), 0)
    try:
        df_results.loc[i, 'year_first_upgrade'] = min(df_temp.funding_year.values)
    except ValueError:
        df_results.loc[i, 'year_first_upgrade'] = 'No upgrade'
    try:
        df_results.loc[i, 'year_latest_upgrade'] = max(df_temp.funding_year.values)
    except ValueError:
        df_results.loc[i, 'year_latest_upgrade'] = 'No upgrade'

# save as a csv
os.chdir(GITHUB + '/''data')
df_results.to_csv('id1018_65districts_upgrade_year_2018sots.csv', index=False)


# for aggregate summary, uncomment the lines below
# df_summary = pd.DataFrame({'num_districts_never_upgrade': df_results[df_results.year_latest_upgrade == 'No upgrade']['district_id'].nunique(),
#                            'num_districts_latest_up_2017': df_results[df_results.year_latest_upgrade == 2017]['district_id'].nunique(),
#                            'num_districts_latest_up_2018': df_results[df_results.year_latest_upgrade == 2018]['district_id'].nunique(),
#                            'num_districts_latest_up_2019': df_results[df_results.year_latest_upgrade == 2019]['district_id'].nunique()},
#                                     index=[0])
