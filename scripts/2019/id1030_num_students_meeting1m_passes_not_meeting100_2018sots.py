'''
Sots 2019 Storyline

First year there are more students at 1Mbps than not at 100kbps
In 2019, 2.4M students do not have access to 100kbps ...
... In 2019, 6.0M students now have access to 1Mbps

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

# get tables from dar prod database
df_d = get_districts()
df_fit = get_districts_fit_for_analysis()
df_bw_cost = get_districts_bw_cost()
cur.close()

# Select subset of columns
df_d_cols = ['district_id', 'funding_year', 'district_type', 'state_code',
             'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_bw_cost_cols = ['district_id', 'funding_year','meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['num_students']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

# filter the dataframe
df_filtered_ia = df[(df.in_universe==True) &
                (df.district_type=='Traditional') &
                (df.fit_for_ia==True)]


# number of students NOT meeting 2014 goal from 2015-2019
df_not_meeting2014 = df_filtered_ia[(df_filtered_ia.meeting_2014_goal_no_oversub == False)]
s1 = df_not_meeting2014.groupby('funding_year')['num_students'].sum()


# number of students meeting 2019 goal from 2015-2019
df_meeting2019 = df_filtered_ia[(df_filtered_ia.meeting_2018_goal_oversub == True)]
s2 = df_meeting2018.groupby('funding_year')['num_students'].sum()

# overall population students (clean and not clean)
s3 = df[(df.in_universe == True) & (df.district_type == 'Traditional')].groupby('funding_year').num_students.sum()

# overall sample_students
s4 = df_filtered_ia.groupby('funding_year').num_students.sum()

# concatenate series into a DataFrame
df_results = pd.concat([s1, s2, s3, s4], axis=1).reset_index()
df_results.columns = ['funding_year', 'num_students_not_meeting_100', 'num_students_meeting_1m','population_students', 'sample_students']

# extrapolate
df_results.loc[:, 'extrapolated_num_students_not_meeting_100'] = (df_results['num_students_not_meeting_100']*df_results['population_students'])/df_results['sample_students']
df_results.loc[:, 'extrapolated_num_students_meeting_1m'] = (df_results['num_students_meeting_1m']*df_results['population_students'])/df_results['sample_students']

# save as a csv
os.chdir(GITHUB + '/Projects/sots-isl/data')
df_results.to_csv('id1030_num_students_meeting1m_passes_not_meeting100_2018sots.csv', index=False)
