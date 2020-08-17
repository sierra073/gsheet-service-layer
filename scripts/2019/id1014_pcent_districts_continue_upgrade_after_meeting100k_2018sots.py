from __future__ import division
import os
import psycopg2
import numpy as np
import pandas as pd
from collections import defaultdict

# connection credentials
HOST = os.environ.get("HOST_DAR_PROD")
USER = os.environ.get("USER_DAR_PROD")
PASSWORD = os.environ.get("PASSWORD_DAR_PROD")
DB = os.environ.get("DB_DAR_PROD")

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
             'locale', 'size', 'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia', 'fit_for_ia_cost']

df_bw_cost_cols = ['district_id', 'funding_year',
                   'ia_bw_mbps_total','ia_bandwidth_per_student_kbps',
                   'ia_monthly_cost_total', 'ia_monthly_cost_per_mbps',
                   'meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']

df_up_cols = ['district_id', 'funding_year', 'upgrade_indicator']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols).merge(df_up[df_up_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['ia_bw_mbps_total','ia_bandwidth_per_student_kbps',
                   'ia_monthly_cost_total', 'ia_monthly_cost_per_mbps']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

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
df_final_ia = pd.merge(df_4years, df_upgrade_counts,  on='district_id')

# Defining cohorts
# initiate results
results = defaultdict(dict)
subset_district_ids = []
double_check = []

# list of districts_ids meeting 100 kbps at anytime between 2015-2019
all_districts_meeting2014 = df_final_ia[df_final_ia.meeting_2014_goal_no_oversub == True].district_id.unique()

for year in [2015, 2017, 2018, 2019]:
    if year == 2015:
        # list of district_ids meeting 100 kpbs in 2015
        districts_meeting2014_year = df_final_ia[(df_final_ia.funding_year == year) &
                                              (df_final_ia.meeting_2014_goal_no_oversub == True)].district_id.unique()
    else:
        reduced_districts_list = set(all_districts_meeting2014).difference(set(subset_district_ids))
        districts_meeting2014_year = df_final_ia[(df_final_ia.funding_year == year) &
                                                 (df_final_ia.district_id.isin(reduced_districts_list)) &
                                                 (df_final_ia.meeting_2014_goal_no_oversub == True)].district_id.unique()


    # check what year each first upgraded
    for d_id in districts_meeting2014_year:
        df_temp = df_final_ia[(df_final_ia.district_id == d_id) &
                               (df_final_ia.upgrade_indicator == True) &
                               (df_final_ia.funding_year > year)]
        if df_temp.shape[0] > 0:
            results[str(d_id)]['year_first_meet2014'] = year
            results[str(d_id)]['year_first_upgrade'] = min(df_temp.funding_year.values)
            results[str(d_id)]['year_latest_upgrade'] = max(df_temp.funding_year.values)
        else:
            results[str(d_id)]['year_first_meet2014'] = year
            results[str(d_id)]['year_first_upgrade'] = year
            results[str(d_id)]['year_latest_upgrade'] = year
    subset_district_ids.extend(districts_meeting2014_year)

# results as df
df_results = pd.DataFrame.from_dict(results, orient='index').reset_index()

# renaming columns
df_results.columns = ['district_id', 'year_first_meet2014', 'year_first_upgrade', 'year_latest_upgrade']

# changing 'district_id' from string to int
df_results['district_id'] = df_results['district_id'].apply(pd.to_numeric)

# adding new column: difference years before upgrade
df_results.loc[:, 'years_to_upgrade_after_meeting100'] = df_results['year_first_upgrade'] - df_results['year_first_meet2014']

# sort by district_id
df_results.sort_values('district_id', inplace=True)


# Revised Metric: XX% Districts continued to upgrade after meeting 100 kbps
# Of districts that met 100kbps goal between 2015-2018, what percentage continued to upgrade after meeting 100 kbps?
num_meeting100_15to17 = df_results[(df_results.year_first_meet2014 < 2019)].district_id.nunique()
num_upgraded = df_results[(df_results.year_first_meet2014 < 2019) &
                          (df_results.years_to_upgrade_after_meeting100 != 0)].district_id.nunique()
print(num_upgraded/num_meeting100_15to17)
