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

# initiate resultant dataframe
df_results = pd.DataFrame(index=np.arange(0, len(df_filtered_ia.state_code.unique())),
                                          columns=['state_code','num_districts_all',
                                                   'num_districts_clean_1518', 'num_districts_3x_1518',
                                                   'num_districts_2x_1518'])
# Looking at bookened years
year1 = 2015
year4 = 2019

# iterate through all states for metrics
for i, state in enumerate(df_filtered_ia.state_code.unique()):
    # filter by state
    df_state = df_filtered_ia[df_filtered_ia.state_code == state]

    # take bookend years
    df_state_year1 = df_state[df_state.funding_year == year1].set_index('district_id')
    df_state_year4 = df_state[df_state.funding_year == year4].set_index('district_id')

    # concatenate the series as a df
    df_state_results = pd.concat([df_state_year1[['ia_bandwidth_per_student_kbps']],
                                  df_state_year4[['ia_bandwidth_per_student_kbps']]], axis=1, sort=False)

    # rename columns
    df_state_results.columns = ['bw_per_student_kbps_2015', 'bw_per_student_kbps_2019']
    df_state_results.reset_index(inplace=True)

    # number of districts overall
    num_districts_all = df_state_results.district_id.nunique()

    # number of districts - clean (removed NaN)
    num_districts_clean = df_state_results[(~df_state_results['bw_per_student_kbps_2015'].isnull()) &
                                           (~df_state_results['bw_per_student_kbps_2019'].isnull())].district_id.nunique()

    # adding new column X times increase from 2015 to 2019
    df_state_results.loc[:, 'bw_Xtimes_15to18'] = df_state_results['bw_per_student_kbps_2019']/df_state_results['bw_per_student_kbps_2015']


    # number of districts that increased bw 3x from 2015 to 2019
    num_districts_3x_1518 = df_state_results[df_state_results.bw_Xtimes_15to18 >= 3].district_id.nunique()

    # number of districts 2x
    num_districts_2x_1518 = df_state_results[df_state_results.bw_Xtimes_15to18 >= 2].district_id.nunique()

    # save results in a df
    df_results.at[i, 'state_code'] = state
    df_results.at[i, 'num_districts_all'] = num_districts_all
    df_results.at[i, 'num_districts_clean_1518'] = num_districts_clean
    df_results.at[i, 'num_districts_3x_1518'] = num_districts_3x_1518
    df_results.at[i, 'num_districts_2x_1518'] = num_districts_2x_1518

# adding percent columns
df_results.loc[:, 'pcent_districts_3x_all'] = df_results['num_districts_3x_1518']/df_results['num_districts_all']
df_results.loc[:, 'pcent_districts_3x_clean'] = df_results['num_districts_3x_1518']/df_results['num_districts_clean_1518']
df_results.loc[:, 'pcent_districts_2x_all'] = df_results['num_districts_2x_1518']/df_results['num_districts_all']
df_results.loc[:, 'pcent_districts_2x_clean'] = df_results['num_districts_2x_1518']/df_results['num_districts_clean_1518']

# remove DC
df_results = df_results[df_results.state_code != 'DC']

# save as a csv
os.chdir(GITHUB + '/''data')
df_results.to_csv('id1007_num_states_pcent_districts_upgrade_3x_2018sots.csv', index=False)
