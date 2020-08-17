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
df_d_cols = ['district_id', 'funding_year',
             'district_type', 'state_code',
             'num_students', 'in_universe']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_bw_cost_cols = ['district_id', 'funding_year',
                    'ia_bw_mbps_total', 'ia_bandwidth_per_student_kbps']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['ia_bandwidth_per_student_kbps', 'ia_bw_mbps_total']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)


# filter the dataframe
df_filtered_ia = df[(df.in_universe==True) &
                 (df.district_type=='Traditional') &
                 (df.fit_for_ia==True)]


# initiate resultant dictionary
series_results = defaultdict(dict)

# iterate through years to find median bw per student kbps
for year in df_filtered_ia.funding_year.unique():
    df_year = df_filtered_ia[df_filtered_ia.funding_year == year]
    series_results[str(year)] = df_year.groupby('state_code')['ia_bandwidth_per_student_kbps'].median()

# save result in a dataframe
df_results = pd.concat([series_results['2015'],
                        series_results['2017'],
                        series_results['2018'],
                        series_results['2019']], sort=False, axis=1).reset_index()

# rename the columns
df_results.columns = ['state_code', 'bw_per_student_kbps_med_2015',
                      'bw_per_student_kbps_med_2017',  'bw_per_student_kbps_med_2018',
                      'bw_per_student_kbps_med_2019']

# add X times columns
# X times from 2015 to 2019
df_results.loc[:, 'bw_Xtimes_15to18'] = df_results['bw_per_student_kbps_med_2019']/df_results['bw_per_student_kbps_med_2015']

# X times from 2017 to 2019
df_results.loc[:, 'bw_Xtimes_16to18'] = df_results['bw_per_student_kbps_med_2019']/df_results['bw_per_student_kbps_med_2017']

# X times from 2018 to 2019
df_results.loc[:, 'bw_Xtimes_17to18'] = df_results['bw_per_student_kbps_med_2019']/df_results['bw_per_student_kbps_med_2018']

# for MMT only: below prints out one value
print(str(df_results[df_results.bw_Xtimes_15to18 >= 3].shape[0]))

# if want full results, uncomment below for a full tables
#df_results.to_csv('id1008_num_states_median_bw_3x_15to18_2018sots_results.csv', index=False)
