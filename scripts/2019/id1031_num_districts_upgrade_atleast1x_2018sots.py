'''
Sots 2019 Storyline Opening Letter

77% of districts took action to upgrade their broadband
'''

from __future__ import division
import os
import psycopg2
import pandas as pd

# credentials for frozen database
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

def get_districts_bw_cost():
    query = "select * from ps.districts_bw_cost"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_bw_cost = pd.DataFrame(rows, columns=names)
    return df_bw_cost

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

# Select subset of columns
df_d_cols = ['district_id', 'funding_year', 'district_type', 'state_code',
             'locale', 'size', 'in_universe']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia', 'fit_for_ia_cost']

df_bw_cost_cols = ['district_id', 'funding_year', 'ia_bw_mbps_total', 'ia_monthly_cost_total',
                   'ia_annual_cost_total', 'ia_monthly_cost_per_mbps', 'ia_bandwidth_per_student_kbps']

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

# percent of districts that upgraded at least 1x
num_total_districts = df_final['district_id'].nunique()
num_districts_upgraded_atleast1x = df_final[df_final.num_times_upgraded > 0]['district_id'].nunique()
pcent_upgrade_atleast1x = num_districts_upgraded_atleast1x/num_total_districts

# summarize in df
df_result = pd.DataFrame({'num_districts_4years_clean': num_total_districts,
'num_districts_upgrade_atleast1x': num_districts_upgraded_atleast1x,
'pcent_districts_upgrade_atleast1x': pcent_upgrade_atleast1x}, index=[0])

# save to csv
os.chdir(GITHUB + '/Projects/sots-isl/data')
df_result.to_csv('id1031_num_districts_upgrade_atleast1x_2018sots.csv', index=False)
