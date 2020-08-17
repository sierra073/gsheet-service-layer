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
results = defaultdict(dict)

# get median bw per student each year
for year in [2015, 2017, 2018, 2019]:
    df_temp = df_filtered[(df_filtered.funding_year == year)]
    results[str(year)]={}
    results[str(year)]['bw_per_student_median'] = round(df_temp.ia_bandwidth_per_student_kbps.median(),2)

df_results = pd.DataFrame.from_dict(results)

# plotting
x = df_results.loc['bw_per_student_median', :].index
y = df_results.loc['bw_per_student_median', :]

plt.figure(figsize=(12, 7))
plt.plot(x, y, marker='o', color='orange')

for i, a, b in zip([0, 1, 2, 3], x, y):
    if i in [0, 3]:
        plt.text(a, b, str(round(b, 2))+ " kbps", color='black')

plt.title("Median Bandwidth Per Student from 2015 to 2019")
plt.ylabel("Kbps Per Student")
plt.margins(0.15);

# save plot
plt.savefig('bw_growth_median_bw_per_student_kbps.png');
print("Process complete")
