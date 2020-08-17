'''
Author: Kat Aquino

Follow up from whiteboarding session on Monday, 7/23/2019
Question: xx state have had xx% of districts or more that have upgraded
Output: .csv with columns:
  - pcent_threshold: float, percent of districts upgraded in 2019
  - num_states_upgrade_at_threshold: int, number of states meeting threshold
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

def get_districts_fit_for_analysis():
    query = "select * from ps.districts_fit_for_analysis"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_fit = pd.DataFrame(rows, columns=names)
    return df_fit

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
df_up = get_districts_upgrades()

cur.close()

# Select subset of columns
df_d_cols = ['district_id', 'funding_year',
             'district_type', 'state_code',
             'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia']

df_up_cols = ['district_id', 'funding_year', 'upgrade_indicator']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                           on=merge_cols).merge(df_up[df_up_cols],
                                                on=merge_cols)

# filter the dataframe
df_filtered_ia = df[(df.in_universe==True) &
                 (df.district_type=='Traditional') &
                 (df.fit_for_ia==True)]

# filter by year
year = 2019
df_2019 = df_filtered_ia[df_filtered_ia.funding_year == 2019]

# number of districts by state
s_num_districts = df_2018.groupby('state_code').district_id.nunique()

# number of districts by state that upgraded
s_num_districts_upgrade = df_2019[df_2018.upgrade_indicator == True].groupby('state_code').district_id.nunique()

# create new resultant dataframe
df_results = pd.concat([s_num_districts, s_num_districts_upgrade], axis=1).reset_index()
df_results.columns = ['state_code', 'num_districts', 'num_districts_upgraded']
df_results.loc[:, 'pcent_districts_upgrade'] = df_results['num_districts_upgraded']/df_results['num_districts']

# replace Nan with zeroes
df_results.fillna(0, inplace=True)

# number of states by percentage of districts that upgraded
num_states_pcent_upgrade = []
for threshold in np.arange(0, 1.05, 0.05):
    num_states_pcent_upgrade.append(df_results[df_results.pcent_districts_upgrade >= threshold].state_code.nunique())

# creating resultant dataframe with number of states by upgrade pcent threshold
df_result_num_by_pcent_upgrade = pd.DataFrame({'pcent_threshold': np.arange(0, 1.05, 0.05),
                                               'num_states_upgrade_at_threshold': num_states_pcent_upgrade})


# save result to csv
os.chdir(GITHUB + '/''data')
df_result_num_by_pcent_upgrade.to_csv('id1006_num_states_by_pcent_districts_upgrade_2018sots.csv', index=False)
