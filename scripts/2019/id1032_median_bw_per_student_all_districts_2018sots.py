# Nearly a third of our school districts have already realized this and have upgraded their networks to these speeds, 
# **while the median school district has 500 kbps of bandwidth per student**


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
df_d_cols = ['district_id', 'funding_year', 'district_type', 'state_code','size',
             'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_bw_cost_cols = ['district_id', 'funding_year', 'ia_bandwidth_per_student_kbps',
                   'meeting_2014_goal_no_oversub',
                   'meeting_2018_goal_oversub']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['num_students', 'ia_bandwidth_per_student_kbps']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

# filter the dataframe
df_filtered_ia = df[(df.in_universe==True) &
                (df.district_type=='Traditional') &
                (df.fit_for_ia==True)]

# median bandwidth per student all districts from 2015-2019
s1 = df_filtered_ia.groupby('funding_year')['ia_bandwidth_per_student_kbps'].median()

# as a df
df_results = s1.to_frame().reset_index()

# save as a table
os.chdir(GITHUB + '/Projects/sots-isl/data')
df_results.to_csv('id1032_median_bw_per_student_all_districts_2018sots.csv', index=False)
