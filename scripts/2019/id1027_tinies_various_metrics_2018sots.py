'''
Storyline Follow ups from Wednesday, August 22, 2019

Of the 48 percent of tinies meeting 1 Mbps - how many are deciding to meet 1 Mbps?
Total IA bw, type of technology, num students - summary stats, histograms, follow-ups.
'''

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

def get_districts_fiber():
    query = "select * from ps.districts_fiber"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_fiber = pd.DataFrame(rows, columns=names)
    return df_fiber

    # get tables from dar prod database
df_d = get_districts()
df_fit = get_districts_fit_for_analysis()
df_bw_cost = get_districts_bw_cost()
df_fiber = get_districts_fiber()

# Select subset of columns
df_d_cols = ['district_id', 'funding_year', 'district_type', 'state_code',
             'locale', 'size', 'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia', 'fit_for_ia_cost']

df_bw_cost_cols = ['district_id', 'funding_year',  'ia_bandwidth_per_student_kbps',
                   'ia_monthly_cost_per_mbps',
                   'ia_bw_mbps_total', 'ia_monthly_cost_total',
                   'meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']

df_fiber_cols = ['district_id', 'funding_year', 'hierarchy_ia_connect_category']

# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols).merge(df_fiber[df_fiber_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['ia_monthly_cost_per_mbps',  'ia_bandwidth_per_student_kbps',
                   'ia_bw_mbps_total', 'ia_monthly_cost_total',]
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)


# filter the dataframe
df_filtered = df[(df.in_universe==True) &
                  (df.district_type=='Traditional') &
                  (df.fit_for_ia==True)]


# National numbers
# number of districts meeting 2019 goal from 2015-2019
df_meeting1m_allyears = df_filtered[(df_filtered.meeting_2018_goal_oversub == True)]
s1 = df_meeting1m_allyears.groupby('funding_year')['district_id'].nunique()

# number of districts total
s2 = df_filtered.groupby('funding_year').district_id.nunique()

# concatenate series into a DataFrame
df_results_national = pd.concat([s1, s2], axis=1).reset_index()

# rename columns
df_results_national.columns = ['funding_year', 'num_districts_meeting1m', 'num_districts_total']

# adding percents
df_results_national.loc[:, 'pcent_districts_meeting1m'] = df_results_national['num_districts_meeting1m']/df_results_national['num_districts_total']


# Tiny districts
# number of districts meeting 2019 goal from 2015-2019
df_meeting1m_allyears_tiny = df_filtered[(df_filtered.meeting_2018_goal_oversub == True) &
                                          (df_filtered['size'] == 'Tiny')]
s3 = df_meeting1m_allyears_tiny.groupby('funding_year')['district_id'].nunique()


# number of districts total (clean)
df_tinies = df_filtered[df_filtered['size'] == 'Tiny']
s4 = df_tinies.groupby('funding_year').district_id.nunique()

# number of districts total (clean and not clean)
df_tinies_all = df[(df.in_universe == True) & (df.district_type == 'Traditional') & (df['size'] == 'Tiny')]
s5 = df_tinies_all.groupby('funding_year').district_id.nunique()

# number of districts meeting 2019 goal from 2015-2019
df_meeting1m_allyears_tiny_stu = df_filtered[(df_filtered.meeting_2018_goal_oversub == True) &
                                             (df_filtered['size'] == 'Tiny')]
s6 = df_meeting1m_allyears_tiny_stu.groupby('funding_year')['num_students'].sum()


# number of students total (clean)
df_tinies_stu = df_filtered[df_filtered['size'] == 'Tiny']
s7 = df_tinies_stu.groupby('funding_year').num_students.sum()

# number of students total (clean and not clean)
df_tinies_all_stu = df[(df.in_universe == True) & (df.district_type == 'Traditional') & (df['size'] == 'Tiny')]
s8 = df_tinies_all_stu.groupby('funding_year').num_students.sum()


# concatenate series into a DataFrame
df_results_tinies = pd.concat([s3, s4, s5, s6, s7, s8], axis=1).reset_index()

# rename columns
df_results_tinies.columns = ['funding_year', 'num_districts_meeting1m_tiny',
                             'num_districts_tiny_sample', 'num_districts_tiny_population',
                             'num_students_meeting1m_tiny',
                             'num_students_tiny_sample', 'num_students_tiny_population']

## number of tiny districts meeting 1m (extrapolated)
df_results_tinies.loc[:, 'num_districts_meeting1m_tiny_extrapolated'] = df_results_tinies['num_districts_meeting1m_tiny']*(df_results_tinies['num_districts_tiny_population']/df_results_tinies['num_districts_tiny_sample'])

# number of students in tiny districts meeting 1m (extrapolated)
df_results_tinies.loc[:, 'num_students_meeting1m_tiny_extrapolated'] = df_results_tinies['num_students_meeting1m_tiny']*(df_results_tinies['num_students_tiny_population']/df_results_tinies['num_students_tiny_sample'])

# adding percents
df_results_tinies.loc[:, 'pcent_districts_meeting1m_tiny'] = df_results_tinies['num_districts_meeting1m_tiny']/df_results_tinies['num_districts_tiny_sample']

# Separating tinies by funding year
df_tinies_2015 = df_tinies[(df_tinies.funding_year == 2015) & (df_tinies.meeting_2018_goal_oversub==True)]
df_tinies_2017 = df_tinies[(df_tinies.funding_year == 2017) & (df_tinies.meeting_2018_goal_oversub==True)]
df_tinies_2018 = df_tinies[(df_tinies.funding_year == 2018) & (df_tinies.meeting_2018_goal_oversub==True)]
df_tinies_2019 = df_tinies[(df_tinies.funding_year == 2019) & (df_tinies.meeting_2018_goal_oversub==True)]

# other metrics for tinies
df_results_bw = pd.DataFrame()
for i, (year, df_temp) in enumerate(zip([2015, 2017, 2018, 2019],
                                        [df_tinies_2015, df_tinies_2017, df_tinies_2018, df_tinies_2019])):
    num_students_temp = df_temp.num_students.sum()
    bw_per_student_kbps_temp = (df_temp.ia_bw_mbps_total.sum()*1000)/num_students_temp
    num_fiber = df_temp[df_temp.hierarchy_ia_connect_category == 'Fiber']['district_id'].nunique()
    df_results_bw.loc[i, 'funding_year'] = year
    df_results_bw.loc[i, 'ia_bw_per_student_kbps_tiny_meeting1m_wavg'] = bw_per_student_kbps_temp
    df_results_bw.loc[i, 'ia_bw_per_student_kbps_tiny_meeting1m_median'] = df_temp.ia_bandwidth_per_student_kbps.median()
    df_results_bw.loc[i, 'num_districts_on_fiber_tiny_meeting1m'] = num_fiber
    df_results_bw.loc[i, 'pcent_districts_on_fiber_tiny_meeting1m'] = num_fiber/df_temp.district_id.nunique()

# concatenate dataframes
df_results_national.set_index('funding_year', inplace=True)
df_results_tinies.set_index('funding_year', inplace=True)
df_results_bw.set_index('funding_year', inplace=True)
df_results_all = pd.concat([df_results_national, df_results_tinies, df_results_bw], axis=1, join='inner')
df_results_all.reset_index(inplace=True)

# save as a table
os.chdir(GITHUB + '/''data')
df_results_all.to_csv('id1027_tinies_various_metrics_2018sots.csv', index=False)
