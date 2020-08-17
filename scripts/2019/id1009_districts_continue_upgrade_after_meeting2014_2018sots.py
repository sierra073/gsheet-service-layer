from __future__ import division

import os
import psycopg2
import numpy as np
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

# functions to get specific tables
def get_districts():
    query = "select * from ps.districts"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_d = pd.DataFrame(rows, columns=names)
    return df_d

def get_districts_bw_cost():
    query = "select * from ps.districts_bw_cost"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_bw_cost = pd.DataFrame(rows, columns=names)
    return df_bw_cost

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

# get tables from dar prod database
df_d = get_districts()
df_fit = get_districts_fit_for_analysis()
df_bw_cost = get_districts_bw_cost()
df_up = get_districts_upgrades()
cur. close()

# Select subset of columns
df_d_cols = ['district_id', 'funding_year','district_type','in_universe']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_bw_cost_cols = ['district_id', 'funding_year','meeting_2014_goal_no_oversub']

df_up_cols = ['district_id', 'funding_year', 'upgrade_indicator']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols).merge(df_up[df_up_cols],
                         on=merge_cols)

# filter the dataframe
df_filtered_ia = df[(df.in_universe==True) &
                 (df.district_type=='Traditional') &
                 (df.fit_for_ia==True)]

# looking only at districts with 4 years of clean data
df_clean_4yrs = df_filtered_ia.groupby('district_id')['funding_year'].count().to_frame().reset_index()
df_clean_4yrs.columns = ['district_id', 'counts_funding_year']


# creating df of districts with 4 years clean data
id_clean_4yrs = df_clean_4yrs[df_clean_4yrs.counts_funding_year == 4].district_id.values
df_4years = df_filtered_ia[df_filtered_ia.district_id.isin(id_clean_4yrs)].sort_values(
                       ['district_id','funding_year'], ascending=[True, True])


# of the districts with clean 4 years, how many times did the district upgrade?
df_upgrade_counts = df_4years.groupby('district_id', as_index=False)['upgrade_indicator'].sum()
df_upgrade_counts.columns = ['district_id', 'num_times_upgraded']

# merge the dataframes
df_final = pd.merge(df_4years, df_upgrade_counts,  on='district_id')

# count number of districts that have met the 2014 goal at sometime between 2015 and 2019
num_districts_meeting2014 = df_final[df_final.meeting_2014_goal_no_oversub == True].district_id.nunique()

# districts meeting 2014 goals: meeting_2014_goal_no_oversub == True
districts_continue = []
for year in [2015, 2017, 2018, 2019]:
    # districts that meet 2014 goal
    ids_meeting_2014 = df_final[(df_final.funding_year == year) & 
                                (df_final.meeting_2014_goal_no_oversub == True)
                               ].district_id.unique()
    # districts that continued to upgrade after meeting 2019 goal
    for id_meet in ids_meeting_2014:
        df_d_meet = df_final[(df_final.funding_year > year) & (df_final.district_id == id_meet)]
        if df_d_meet.upgrade_indicator.sum() > 0:
            districts_continue.append(id_meet)
        else: 
            continue
            
# list of districts that continued upgrading after meeting 2014 goal
districts_continue_after_meet_2014 = set(districts_continue)
num_districts_met_2014_and_continue = len(districts_continue_after_meet_2014)

# result: percentage of districts that continued to upgrade after meeting 100kbps
print(round((num_districts_met_2014_and_continue/num_districts_meeting2014)*100, 2))
