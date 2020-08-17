'''
Sots 2019 National Analysis Draft

In Arkansas, the state set a goal of bringing 1 Mbps per student of Internet access capacity
to every school and delivered this to 98 percent of their schools in 2017. (Future, p13)

This is a slightly different version from id1024 where that looks at districts, 
this covers schools.
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

def get_districts_bw_cost():
    query = "select * from ps.districts_bw_cost"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_bw_cost = pd.DataFrame(rows, columns=names)
    return df_bw_cost

# get tables from dar prod database
df_d = get_districts()
df_fit = get_districts_fit_for_analysis()
df_bw_cost = get_districts_bw_cost()

# Select subset of columns
df_d_cols = ['district_id', 'name','funding_year', 'district_type', 'state_code',
             'locale', 'size', 'in_universe', 'num_students', 'num_schools']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_bw_cost_cols = ['district_id', 'funding_year', 'meeting_2018_goal_oversub', 'ia_bandwidth_per_student_kbps']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['num_schools']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

# filter the dataframe
df_filtered = df[(df.in_universe==True) &
                  (df.district_type=='Traditional') &
                  (df.fit_for_ia==True)]

# looking at districts in AR in fy 2019
fy = 2019
state = 'AR'
df_AR_2019 = df_filtered[(df_filtered.funding_year == fy) &
                         (df_filtered.state_code == state)]

num_total_schools_AR = df_AR_2018.num_schools.sum()
num_schools_meeting1m_oversub = df_AR_2019[df_AR_2018.meeting_2018_goal_oversub == True].num_schools.sum()
num_schools_meeting1m_no_oversub = df_AR_2019[df_AR_2018.ia_bandwidth_per_student_kbps >= 1000].num_schools.sum()

# summary
df_summary = pd.DataFrame({'state_code': state,
                           'funding_year': fy,
                           'num_schools_total': num_total_schools_AR,
                           'num_schools_meeting1m_oversub': num_schools_meeting1m_oversub,
                           'num_schools_meeting1m_no_oversub': num_schools_meeting1m_no_oversub,
                           'pcent_schools_meeting1m_oversub': round(num_schools_meeting1m_oversub/num_total_schools_AR, 2),
                           'pcent_schools_meeting1m_no_oversub': round(num_schools_meeting1m_no_oversub/num_total_schools_AR, 2)},
                          index=[0])

# save as a csv
os.chdir(GITHUB + '/Projects/sots-isl/data')
df_summary.to_csv('id1035_schools_in_AR_meeting1m_2018sots.csv', index=False)
