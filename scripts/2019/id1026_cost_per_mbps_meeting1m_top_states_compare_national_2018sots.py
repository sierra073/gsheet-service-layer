# ### Storyline Follow ups from Wednesday, August 22, 2019
#
# - Cost per Mbps of those meeting 1 Mbps 2015, 2017, 2018, 2019 and then compared to the top states (JMB has list - AR, NM, OK, WI, ME, etc.. ) and overall spending comparison - did come down meaningfully faster? And in a way that they did not need to spend more money (nice to have)?

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

df_bw_cost_cols = ['district_id', 'funding_year', 'ia_monthly_cost_per_mbps',
                   'meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['ia_monthly_cost_per_mbps']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)


# filter the dataframe
df_filtered = df[(df.in_universe==True) &
                  (df.district_type=='Traditional') &
                  (df.fit_for_ia==True) &
                 (df.fit_for_ia_cost == True)]

# national: cost per mbps for those meeting 1M from 2015-2019
df_meeting2019 = df_filtered[(df_filtered.meeting_2018_goal_oversub == True)]
s1 = df_meeting2018.groupby('funding_year')['ia_monthly_cost_per_mbps'].median()

# by State
# cost per mbps for those meeting 1M from 2015-2019 for top states
df_meeting2019_top_states = df_filtered[(df_filtered.meeting_2018_goal_oversub == True) &
                            (df_filtered.state_code.isin(['AR', 'NM', 'OK', 'WI', 'ME']))]

s2 = df_meeting2019_top_states.groupby(['funding_year', 'state_code'])['ia_monthly_cost_per_mbps'].median()

# join multiindex into one for easy plotting
s2.index = ['_'.join(list((str(ind[0]), ind[1]))) for i, ind in enumerate(s2.index)]

# plotting
plt.figure(figsize=(12, 7))

# plotting national
x_nat = s1.index
y_nat = s1.values
plt.plot(x_nat, y_nat, marker='o', label='national', color='#cccccc')
for i, a, b in zip(np.arange(0, len(x_nat)), x_nat, y_nat):
    if i in [0, 3]:
        plt.text(a, b, str(round(b, 2)), color='#cccccc',
        verticalalignment="bottom", horizontalalignment="left");

# by state
colors = ['#461000', '#83351a', '#c44f27', '#f26c23', '#f9a677']
states = ['AR', 'NM', 'OK', 'WI', 'ME']
for j, c, state in zip(np.arange(0, len(states)), colors, states):
    fy_vals = [fy for fy in s2.index if state in fy]
    y_state = [s2[fy] for fy in fy_vals]
    plt.plot(x_nat, y_state, marker='o', label=state, color=c)
    for i, a, b in zip(np.arange(0, len(x_nat)), x_nat, y_state):
        if i in [0, 3]:
            if j % 2 == 0:
                plt.text(a, b, str(round(b, 2)), color=c,
                verticalalignment="bottom", horizontalalignment="left");
            else:
                plt.text(a, b, str(round(b, 2)), color=c,
                verticalalignment="bottom", horizontalalignment="right");


plt.xticks(x_nat, [2015, 2017, 2018, 2019])
plt.ylabel("Cost Per Mbps (median)")
plt.title("Cost Per Mbps of Districts Meeting 1 Mbps Goal: National vs. Top States")
plt.legend()
plt.margins(0.25);

# save figure
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id1026_cost_per_mbps_meeting1m_top_states_compare_national_2018sots.png', bbox_inches = 'tight')
