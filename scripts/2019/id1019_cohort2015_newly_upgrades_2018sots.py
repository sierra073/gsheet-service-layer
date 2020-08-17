'''
PURPOSE: Sots 2019 Follow Up: Preparing for the Future
Cohort 2015 - find number of newly upgrades
(if in 2017, cannot be in 2018 and/or in 2019) -
how many of the people meeting goals upgraded in one of the years?
'''

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
             'locale', 'size', 'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia', 'fit_for_ia_cost']

df_bw_cost_cols = ['district_id', 'funding_year',
                   'meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']

df_up_cols = ['district_id', 'funding_year', 'upgrade_indicator']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols).merge(df_up[df_up_cols],
                         on=merge_cols)

# filter the dataframe
df_filtered_both = df[(df.in_universe==True) &
                  (df.district_type=='Traditional') &
                  (df.fit_for_ia==True) &
                  (df.fit_for_ia_cost==True)]


# looking only at districts with 4 years of clean data
df_clean_4yrs = df_filtered_both.groupby('district_id')['funding_year'].count().to_frame().reset_index()
df_clean_4yrs.columns = ['district_id', 'counts_funding_year']


# creating df of districts with 4 years clean data
id_clean_4yrs = df_clean_4yrs[df_clean_4yrs.counts_funding_year == 4].district_id.values
df_4years = df_filtered_both[df_filtered_both.district_id.isin(id_clean_4yrs)].sort_values(
                       ['district_id','funding_year'], ascending=[True, True])


# of the districts with clean 4 years, how many times did the district upgrade?
df_upgrade_counts = df_4years.groupby('district_id', as_index=False)['upgrade_indicator'].sum()
df_upgrade_counts.columns = ['district_id', 'num_times_upgraded']

# merge the dataframes
df_final = pd.merge(df_4years, df_upgrade_counts,  on='district_id')

# initiate results
results = defaultdict(dict)
subset_district_ids = []

# list of districts_ids meeting 100 kbps at anytime between 2015-2019
all_districts_meeting2014 = df_final[df_final.meeting_2014_goal_no_oversub == True].district_id.unique()

for year in [2015, 2017, 2018, 2019]:
    if year == 2015:
        # list of district_ids meeting 100 kpbs in 2015
        districts_meeting2014_year = df_final[(df_final.funding_year == year) &
                                              (df_final.meeting_2014_goal_no_oversub == True)].district_id.unique()
    else:
        reduced_districts_list = set(all_districts_meeting2014).difference(set(subset_district_ids))
        districts_meeting2014_year = df_final[(df_final.funding_year == year) &
                                              (df_final.district_id.isin(reduced_districts_list)) &
                                              (df_final.meeting_2014_goal_no_oversub == True)].district_id.unique()

    # check what year each first upgraded
    for d_id in districts_meeting2014_year:
        df_temp = df_final[(df_final.district_id == d_id) &
                           (df_final.upgrade_indicator == True) &
                           (df_final.funding_year > year)]
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
df_results.columns = ['district_id', 'year_first_meet2014', 'year_latest_upgrade', 'year_first_upgrade']

# changing 'district_id' from string to int
df_results['district_id'] = df_results['district_id'].apply(pd.to_numeric)

# adding new column: difference years before upgrade
df_results.loc[:, 'years_to_upgrade_after_meeting100'] = df_results['year_first_upgrade'] - df_results['year_first_meet2014']

# sort by district_id
df_results.sort_values('district_id', inplace=True)

# for cohort 2015, but can be modified for any cohort year
cohort_year = 2015
df_2015 = df_results[df_results.year_first_meet2014 == cohort_year]
num_d_2015 = df_2015.district_id.nunique()

# of the ones that upgraded, what is the average years
df_2015_up = df_2015[df_2015.years_to_upgrade_after_meeting100 != 0]
avg_years_upgrade = df_2015_up.years_to_upgrade_after_meeting100.sum()/df_2015_up.district_id.nunique()
num_first_up_2017 = df_2015[df_2015.year_first_upgrade == 2017].district_id.nunique()
num_first_up_2018 = df_2015[df_2015.year_first_upgrade == 2018].district_id.nunique()
num_first_up_2019 = df_2015[df_2015.year_first_upgrade == 2019].district_id.nunique()
num_0x_up = df_2015[df_2015.year_first_upgrade == 2015].district_id.nunique()

# summary
df_summary = pd.DataFrame({'num_districts_cohort2015': num_d_2015,
                           'num_districts_newly_upgrade_2017': num_first_up_2017,
                           'num_districts_newly_upgrade_2018': num_first_up_2018,
                           'num_districts_newly_upgrade_2019': num_first_up_2019,
                           'num_districts_never_upgrade': num_0x_up}, index=[0])

# save as a csv
os.chdir(GITHUB + '/Projects/sots-isl/data')
df_summary.to_csv('id1019_cohort2015_newly_upgrades_2018sots.csv', index=False)