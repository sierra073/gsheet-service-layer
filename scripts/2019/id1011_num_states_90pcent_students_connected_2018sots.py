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
myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB)
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


# get tables from dar prod database
df_d = get_districts()
df_fit = get_districts_fit_for_analysis()
df_bw_cost = get_districts_bw_cost()
cur. close()

# Select subset of columns
df_d_cols = ['district_id', 'funding_year', 'district_type', 'state_code',
             'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_bw_cost_cols = ['district_id', 'funding_year', 'meeting_2014_goal_no_oversub']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                           on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                                                on=merge_cols)

# change to numeric columns
numeric_cols = ['num_students']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

# filter the dataframe
df_filtered_ia = df[(df.in_universe == True) &
                    (df.district_type == 'Traditional') &
                    (df.fit_for_ia == True)]


# Part 1: Calculating the percent of students meeting 2014 goals by state by each year (2015-2019)
# meeting 100k == 'meeting_2014_goal_no_oversub' == True
df_results_all = defaultdict(dict)
for year in df_filtered_ia.funding_year.unique():
    df_meeting_100k_by_year = df_filtered_ia[(df_filtered_ia.funding_year == year) &
                                             (df_filtered_ia.meeting_2014_goal_no_oversub == True)]
    df_all_by_year = df_filtered_ia[(df_filtered_ia.funding_year == year)]
    # all districts
    s1_all = df_all_by_year.groupby('state_code').num_students.sum()
    s2_all = df_all_by_year.groupby('state_code').district_id.nunique()
    s1_100k = df_meeting_100k_by_year.groupby('state_code').num_students.sum()
    s2_100k = df_meeting_100k_by_year.groupby('state_code').district_id.nunique()
    df_results_all[str(year)] = pd.concat([s1_all, s2_all, s1_100k, s2_100k], sort=True, axis=1).reset_index()
    df_results_all[str(year)].columns = ['state_code', 'num_students_all',
                                         'num_districts_all', 'num_students_meeting2014',
                                         'num_districts_meeting2014']
    df_results_all[str(year)]['pcent_students_meeting2014_' + 'fy' + str(year)] = df_results_all[str(year)]['num_students_meeting2014'] / df_results_all[str(year)]['num_students_all']


# Part 2:  Calculating XX States have connected 90% of total students in 2019
df_2019 = df_results_all['2019']

# remove DC
df_2019 = df_2019[df_2018.state_code != 'DC']

# States in 2019 where 90% of students are meeting the 100 kpbs goal
threshold = 0.9
num_states_90 = df_2019[df_2018.pcent_students_meeting2014_fy2019 >= threshold].shape[0]

# states_meeting_2014goal_2019 = list(df_2019[df_2018.pcent_students_meeting2014_fy2019 >= threshold].state_code.values)

# # States in 2019 where 90% of students are NOT meeting the 100 kpbs goal
# states_not_meeting_in_2019 = set(df_2018.state_code.values).difference(states_meeting_2014goal_2019)


# #Part 3: Calculating XX States have connected 90% of total students in 2015 (if want to compare to 2019)
# df_2015 = df_results_all['2015']

# # remove DC
# df_2015 = df_2015[df_2015.state_code != 'DC']

# threshold = 0.9
# num_states_90 = df_2015[df_2015.pcent_students_meeting2014_fy2015 >= threshold].shape[0]
# states_meeting_2014goal_2015 = list(df_2015[df_2015.pcent_students_meeting2014_fy2015 >= threshold].state_code.values)

print(str(num_states_90))
