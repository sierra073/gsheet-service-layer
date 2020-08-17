# ### Storyline Follow ups from Wednesday, August 22, 2019
#
# - Bending the curve 1 Mbps meeting - viz - % meeting compare national to top states again
#     - Percent of all districts meeting 1m over the 4 years
#     - For specific states (AR, NM, OK, WI, ME) - what percent of their districts meeting 1m?

from __future__ import division
import os
import psycopg2
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

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
df_d_cols = ['district_id', 'funding_year', 'district_type', 'state_code',
             'locale', 'size', 'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia', 'fit_for_ia_cost']

df_bw_cost_cols = ['district_id', 'funding_year',
                   'meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols)

# filter the dataframe
df_filtered = df[(df.in_universe==True) &
                  (df.district_type=='Traditional') &
                  (df.fit_for_ia==True) &
                  (df.state_code != 'DC')]

# national: percent of districts meeting 1M from 2015-2019
# number of districts meeting 2019 goal from 2015-2019
df_meeting2019 = df_filtered[(df_filtered.meeting_2018_goal_oversub == True)]
s1 = df_meeting2018.groupby('funding_year')['district_id'].nunique()


# number of districts total
s2 = df_filtered.groupby('funding_year').district_id.nunique()

# concatenate series into a DataFrame
df_results_national = pd.concat([s1, s2], axis=1).reset_index()

# rename columns
df_results_national.columns = ['funding_year', 'num_districts_meeting1m', 'num_districts_total']

# adding percents
df_results_national.loc[:, 'pcent_districts_meeting1m'] = df_results_national['num_districts_meeting1m']/df_results_national['num_districts_total']


# ### by State
# number of districts meeting 2019 goal from 2015-2019 for states
df_meeting2019_top_states = df_filtered[(df_filtered.meeting_2018_goal_oversub == True) &
                                (df_filtered.state_code.isin(['AR', 'NM', 'OK', 'WI', 'ME']))
                               ]
s3 = df_meeting2019_top_states.groupby(['funding_year', 'state_code'])['district_id'].nunique()

# join indices to combine series
s3.index = ['_'.join(list((str(ind[0]), ind[1]))) for i, ind in enumerate(s3.index)]

# number of districts in top states 'AR', 'NM', 'OK', 'WI', 'ME'
df_top_states = df_filtered[(df_filtered.state_code.isin(['AR', 'NM', 'OK', 'WI', 'ME']))]
s4 = df_top_states.groupby(['funding_year', 'state_code'])['district_id'].nunique()

# join indices to combine series
s4.index = ['_'.join(list((str(ind[0]), ind[1]))) for i, ind in enumerate(s4.index)]

# concatenate series into a DataFrame
df_results_top_states = pd.concat([s3, s4], axis=1).reset_index()

# rename columns
df_results_top_states.columns = ['funding_year', 'num_districts_meeting1m_by_state', 'num_districts_total_by_state']

# add percent columns
df_results_top_states.loc[:, 'pcent_districts_meeting1m_by_state'] = df_results_top_states['num_districts_meeting1m_by_state']/df_results_top_states['num_districts_total_by_state']


# ### Plotting

plt.figure(figsize=(12, 7))

# national plot
x_nat = df_results_national.funding_year
y_nat = df_results_national.pcent_districts_meeting1m
plt.plot(x_nat, y_nat, marker='o', label='national', color='#cccccc')
for i, a, b in zip(np.arange(0, len(x_nat)), x_nat, y_nat):
    if i in [0, 3]:
        plt.text(a, b, str(round(b*100, 2)), color='#cccccc',
        verticalalignment="bottom", horizontalalignment="center");
plt.ylim(-0.1, 1.1)
plt.xticks(x_nat, ['2015', '2017', '2018', '2019'])
plt.margins(0.25)

# by state
colors = ['#461000', '#83351a', '#c44f27', '#f26c23', '#f9a677']
states = ['AR', 'NM', 'OK', 'WI', 'ME']
for j, c, state in zip(np.arange(0, len(states)), colors, states):
    fy_vals = [fy for fy in df_results_top_states.funding_year.values if state in fy]
    y_state = [df_results_top_states[df_results_top_states.funding_year == x].pcent_districts_meeting1m_by_state.values[0]
            for x in fy_vals]
    plt.plot(x_nat, y_state, marker='o', label=state, color=c)
    for i, a, b in zip(np.arange(0, len(x_nat)), x_nat, y_state):
        if i in [0, 3]:
            if j % 2 == 0:
                plt.text(a, b, str(round(b*100, 2)), color=c,
                verticalalignment="bottom", horizontalalignment="left");
            else:
                plt.text(a, b, str(round(b*100, 2)), color=c,
                verticalalignment="bottom", horizontalalignment="right");

plt.title("Percent Meeting 1 Mbps Goal: National vs. Top States")
plt.legend();

# save figure
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig("id1025_pcent_meeting1m_top_states_compare_national_2018sots.png", bbox_inches = 'tight')
