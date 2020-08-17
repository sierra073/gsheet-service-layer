import os
import psycopg2
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

import get_dar_prod_tables as get_tables

# get tables from dar prod database
df_d = get_tables.get_districts()
df_fit = get_tables.get_districts_fit_for_analysis()
df_bw_cost = get_tables.get_districts_bw_cost()
df_up = get_tables.get_districts_upgrades()
print("Retrieving tables...")


# Select subset of columns
df_d_cols = ['district_id', 'funding_year', 'district_type', 'state_code',
             'locale', 'size', 'in_universe']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia', 'fit_for_ia_cost']

df_bw_cost_cols = ['district_id', 'funding_year', 'ia_bw_mbps_total', 'ia_monthly_cost_total',
                   'ia_annual_cost_total', 'ia_monthly_cost_per_mbps', 'ia_bandwidth_per_student_kbps',
                   'meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']

df_up_cols = ['district_id', 'funding_year', 'upgrade_indicator']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols).merge(df_up[df_up_cols],
                         on=merge_cols)


# filter the database
df_filtered = df[(df.in_universe==True) &
                  (df.district_type=='Traditional') &
                  (df.fit_for_ia==True)]

# initiate result
results_goal = defaultdict(dict)
upgrade_ids = []

for year in [2017, 2018, 2019]:
    df_temp_2014 = df_filtered[(df_filtered.funding_year == year) & (df_filtered.meeting_2014_goal_no_oversub == True)]
    df_temp_2019 = df_filtered[(df_filtered.funding_year == year) & (df_filtered.meeting_2018_goal_oversub == True)]
    num_meeting_2014 = df_temp_2014.shape[0]
    num_meeting_2019 = df_temp_2018.shape[0]
    results_goal[str(year)]={}
    results_goal[str(year)]['num_meeting_2014'] = num_meeting_2014
    results_goal[str(year)]['pcent_meeting_2014'] = num_meeting_2014/df_filtered[(df_filtered.funding_year == year)].shape[0]
    results_goal[str(year)]['num_meeting_2019'] = num_meeting_2019
    results_goal[str(year)]['pcent_meeting_2019'] = num_meeting_2019/df_filtered[(df_filtered.funding_year == year)].shape[0]

df_results_goal = pd.DataFrame.from_dict(results_goal)

x0 = df_results_goal.loc['pcent_meeting_2014', :].index
y0 = df_results_goal.loc['pcent_meeting_2014', :]

x1 = df_results_goal.loc['pcent_meeting_2019', :].index
y1 = df_results_goal.loc['pcent_meeting_2019', :]

plt.figure(figsize=(12, 7))
plt.plot(x0, y0, marker='o', color='grey', label='Meeting 2014 Goal')
for i, a, b in zip([0, 1, 2, 3], x0, y0):
    if i in [0, 2]:
        plt.text(a, b, str(round(b, 2)*100)+'%', color='black')


plt.plot(x1, y1, marker='o', color='orange', label='Meeting 2019 Goal')
for i, a, b in zip([0, 1, 2, 3], x0, y1):
    if i in [0, 2]:
        plt.text(a, b, str(round(b, 1)*100)+'%', color='black')


plt.title("Percent of Districts Meeting Bandwidth Goals from 2017 to 2019")
plt.ylabel("Percent")
plt.ylim(0, 1)
plt.margins(x=0.25, y=0.25)
plt.legend(loc='best');

# # save plot
plt.savefig('bw_growth_pcent_districts_meeting_goal_2018sotss.png');
print("Process complete")
