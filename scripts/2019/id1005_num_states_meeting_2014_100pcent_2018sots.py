'''
Author: Kat Aquino

Follow up from whiteboarding session on Monday, 7/23/2019
Question: XX States have connected 99% of total students for fy 2019
Output:
1. Number of states that have connected 100% of students
2. State code of states that have connected 100% of students
'''

import os
import psycopg2
import numpy as np
import pandas as pd

# connection credentials
HOST = os.environ.get("HOST_DAR_PROD")
USER = os.environ.get("USER_DAR_PROD")
PASSWORD = os.environ.get("PASSWORD_DAR_PROD")
DB = os.environ.get("DB_DAR_PROD")

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


# get tables from dar prod database
df_d = get_districts()
df_fit = get_districts_fit_for_analysis()
df_bw_cost = get_districts_bw_cost()

cur.close()

# Select subset of columns
df_d_cols = ['district_id', 'funding_year', 'district_type', 'state_code',
             'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_bw_cost_cols = ['district_id', 'funding_year', 'meeting_2014_goal_no_oversub',]


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols)

# filter the dataframe
df_filtered_ia = df[(df.in_universe==True) &
                 (df.district_type=='Traditional') &
                 (df.fit_for_ia==True)]

# meeting 100k == 'meeting_2014_goal_no_oversub' == True
# create 2019 df and districts meeting 2014 goal in 2019
year = 2019
df_meeting_100k_2019 = df_filtered_ia[(df_filtered_ia.funding_year == year) &
                                         (df_filtered_ia.meeting_2014_goal_no_oversub == True)]
df_all_by_year = df_filtered_ia[(df_filtered_ia.funding_year == year)]

# group by state
s1_all = df_all_by_year.groupby('state_code').num_students.sum()
s2_all = df_all_by_year.groupby('state_code').district_id.nunique()
s1_100k = df_meeting_100k_2019.groupby('state_code').num_students.sum()
s2_100k = df_meeting_100k_2019.groupby('state_code').district_id.nunique()

# create new resultant dataframe
df_results = pd.concat([s1_all, s2_all, s1_100k, s2_100k], axis=1).reset_index()
df_results.columns = ['state_code', 'num_students_all', 'num_districts_all',
                      'num_students_meeting_2014', 'num_districts_meeting_2014']

# create column of percent of students meeting 2014 goal
df_results['pcent_students_meeting_2014_'+'fy'+str(year)] = df_results['num_students_meeting_2014']/df_results['num_students_all']

# select states meeting 99% or more percent students meeting 2014 goal
num_states_100 = df_results[df_results.pcent_students_meeting_2014_fy2019 >= 1.0].shape[0]
print(str(num_states_100))
