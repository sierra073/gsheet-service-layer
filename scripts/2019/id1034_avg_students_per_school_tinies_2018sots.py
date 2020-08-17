'''
2019 Sots National Analysis Draft

With an average of only 200 students per school and without
the resources to offer the same educational opportunities to their students
as larger school districts (Future, p13)
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
df_d_cols = ['district_id', 'funding_year', 'district_type', 'size', 'in_universe', 'num_students', 'num_schools']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_bw_cost_cols = ['district_id', 'funding_year',
                   'meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['num_students', 'num_schools']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)


# # filter the dataframe
# df_filtered = df[(df.in_universe==True) &
#                   (df.district_type=='Traditional') &
#                   (df.fit_for_ia==True) &
#                   (df.fit_for_ia_cost == True)]

# filter the dataframe
df_filtered = df[(df.in_universe==True) &
                  (df.district_type=='Traditional') &
                  (df.fit_for_ia==True)]


# Tiny districts: Schools
# number of districts meeting 2019 goal from 2015-2019
df_meeting1m_allyears_tiny = df_filtered[(df_filtered.meeting_2018_goal_oversub == True) &
                                          (df_filtered['size'] == 'Tiny')]
s1 = df_meeting1m_allyears_tiny.groupby('funding_year')['num_schools'].sum()


# number of schools total (clean)
df_tinies = df_filtered[df_filtered['size'] == 'Tiny']
s2 = df_tinies.groupby('funding_year').num_schools.sum()

# number of schools total (clean and not clean)
df_tinies_all = df[(df.in_universe == True) & (df.district_type == 'Traditional') & (df['size'] == 'Tiny')]
s3 = df_tinies_all.groupby('funding_year').num_schools.sum()

# Tiny districts: Students
# number of districts meeting 2019 goal from 2015-2019
df_meeting1m_allyears_tiny_stu = df_filtered[(df_filtered.meeting_2018_goal_oversub == True) &
                                             (df_filtered['size'] == 'Tiny')]
s4 = df_meeting1m_allyears_tiny_stu.groupby('funding_year')['num_students'].sum()


# number of students total (clean)
df_tinies_stu = df_filtered[df_filtered['size'] == 'Tiny']
s5 = df_tinies_stu.groupby('funding_year').num_students.sum()

# number of students total (clean and not clean)
df_tinies_all_stu = df[(df.in_universe == True) & (df.district_type == 'Traditional') & (df['size'] == 'Tiny')]
s6 = df_tinies_all_stu.groupby('funding_year').num_students.sum()


# concatenate series into a DataFrame
df_results_tinies = pd.concat([s1, s2, s3, s4, s5, s6], axis=1).reset_index()

# rename columns
df_results_tinies.columns = ['funding_year', 'num_schools_meeting1m_tiny',
                             'num_schools_tiny_sample', 'num_schools_tiny_population',
                             'num_students_meeting1m_tiny',
                             'num_students_tiny_sample', 'num_students_tiny_population']

# add average number of students per school in districts meeting 1 Mbps
df_results_tinies.loc[:, 'avg_students_per_school'] = df_results_tinies['num_students_meeting1m_tiny']/df_results_tinies['num_schools_meeting1m_tiny']

# save as a table
os.chdir(GITHUB + '/''data')
df_results_tinies.to_csv("id1034_avg_students_per_school_tinies_2018sots.csv", index=False)
