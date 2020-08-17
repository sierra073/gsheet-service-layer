'''
Storyline Follow ups from Wednesday, August 22, 2019

- Histogram of bw/student for 2019 (part of cohort analysis)
'''

from __future__ import division
import os
import psycopg2
import numpy as np
import pandas as pd
from collections import defaultdict
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
df_bw_cost = get_districts_bw_cost()
df_up = get_districts_upgrades()

# Select subset of columns
df_d_cols = ['district_id', 'funding_year', 'district_type', 'state_code',
             'locale', 'size', 'in_universe', 'num_students']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia', 'fit_for_ia_cost']

df_bw_cost_cols = ['district_id', 'funding_year', 'ia_bandwidth_per_student_kbps',
                   'meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']

df_up_cols = ['district_id', 'funding_year', 'upgrade_indicator']

# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols).merge(df_up[df_up_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['ia_bandwidth_per_student_kbps']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

# filter the dataframe
df_filtered_ia = df[(df.in_universe==True) &
                 (df.district_type=='Traditional') &
                 (df.fit_for_ia==True)]

# looking only at districts with 4 years of clean data
df_clean_4yrs = df_filtered_ia.groupby('district_id')['funding_year'].count().to_frame().reset_index()
df_clean_4yrs.columns = ['district_id', 'counts_funding_year']

# creating df of districts with 4 years clean data
id_clean_4yrs = df_clean_4yrs[df_clean_4yrs.counts_funding_year == 4].district_id.values
df_4years = df_filtered_ia[df_filtered_ia.district_id.isin(id_clean_4yrs)].sort_values(
                       ['district_id','funding_year'], ascending=[True, True])


# of the districts with clean 4 years, how many times did the district upgrade?
df_upgrade_counts = df_4years.groupby('district_id', as_index=False)['upgrade_indicator'].sum()
df_upgrade_counts.columns = ['district_id', 'num_times_upgraded']

# merge the dataframes
df_final = pd.merge(df_4years, df_upgrade_counts,  on='district_id')

# Defining cohorts
# initiate results
results = defaultdict(dict)
subset_district_ids = []

# list of districts_ids meeting 100 kbps at anytime between 2015-2019
all_districts_meeting2014 = df_final[df_final.meeting_2014_goal_no_oversub == True].district_id.unique()

for year in [2015, 2017, 2018, 2019]:
    if year == 2015:
        # list of district_ids meeting 100 kpbs in 2015
        districts_meeting2014_year = df_final[(df_final.funding_year == year) &
                                              (df_final.meeting_2014_goal_no_oversub == True)].district_id.unique()
    else:
        reduced_districts_list = set(all_districts_meeting2014).difference(set(subset_district_ids))
        districts_meeting2014_year = df_final[(df_final.funding_year == year) &
                                              (df_final.district_id.isin(reduced_districts_list)) &
                                              (df_final.meeting_2014_goal_no_oversub == True)].district_id.unique()

    # check what year each first upgraded
    for d_id in districts_meeting2014_year:
        df_temp = df_final[(df_final.district_id == d_id) &
                           (df_final.upgrade_indicator == True) &
                           (df_final.funding_year > year)]
        if df_temp.shape[0] > 0:
            results[str(d_id)]['year_first_meet2014'] = year
            results[str(d_id)]['year_first_upgrade'] = min(df_temp.funding_year.values)
            results[str(d_id)]['year_latest_upgrade'] = max(df_temp.funding_year.values)
        else:
            results[str(d_id)]['year_first_meet2014'] = year
            results[str(d_id)]['year_first_upgrade'] = year
            results[str(d_id)]['year_latest_upgrade'] = year
    subset_district_ids.extend(districts_meeting2014_year)


# results as df
df_results = pd.DataFrame.from_dict(results, orient='index').reset_index()

# renaming columns
df_results.columns = ['district_id', 'year_first_meet2014', 'year_first_upgrade', 'year_latest_upgrade']

# changing 'district_id' from string to int
df_results['district_id'] = df_results['district_id'].apply(pd.to_numeric)

# adding new column: difference years before upgrade
df_results.loc[:, 'years_to_upgrade_after_meeting100'] = df_results['year_first_upgrade'] - df_results['year_first_meet2014']

# sort by district_id
df_results.sort_values('district_id', inplace=True)

# set index with district_id for upcoming left join
df_final_temp = df_final.set_index('district_id')
df_results_temp = df_results.set_index('district_id')

# left join df_results with df_final funding year 2019
df_results_all = pd.concat([df_results_temp, df_final_temp[(df_final_temp.funding_year == 2019)]],
                           axis=1, join_axes=[df_results_temp.index])

# reset index
df_results_all.reset_index(inplace=True)


# Separate into cohort groups
df_2015 = df_results_all[df_results_all.year_first_meet2014 == 2015]
df_2017 = df_results_all[df_results_all.year_first_meet2014 == 2017]
df_2018 = df_results_all[df_results_all.year_first_meet2014 == 2018]
df_2019 = df_results_all[df_results_all.year_first_meet2014 == 2019]


# histograms of bw per student by cohort
# set figure parameters
fig = plt.figure(figsize=(13, 5))
ax1 = fig.add_axes([0.1, 0.5, 0.8, 0.4],
                   xticklabels=[], xlim=(0, 5000), ylim=(0, 0.003))
ax2 = fig.add_axes([0.1, 0.1, 0.8, 0.4], xlim=(0, 5000),
                   xticklabels=[], ylim=(0, 0.003))
ax3 = fig.add_axes([0.1, -0.3, 0.8, 0.4], xlim=(0, 5000),
                   xticklabels=[], ylim=(0, 0.003))
ax4 = fig.add_axes([0.1, -0.7, 0.8, 0.4], xlim=(0, 5000),
                   ylim=(0, 0.003))

# cohort 2015 plot: histogram, median vertical line, kde
ax1.hist(df_2015['ia_bandwidth_per_student_kbps'],
         bins = 1000,
         color = '#c44f27',
         edgecolor = 'black',
         normed=True, label='Cohort 2015');

ax1.axvline(df_2015['ia_bandwidth_per_student_kbps'].median(),
            ymin=0, ymax=1, linestyle=':',
            label=str(round(df_2015['ia_bandwidth_per_student_kbps'].median(), 2))+ " kbps")
df_2015['ia_bandwidth_per_student_kbps'].plot(kind='kde', ax=ax1, color='darkcyan',label='');
ax1.legend()

# cohort 2017 plot: histogram, median vertical line, kde
ax2.hist(df_2017['ia_bandwidth_per_student_kbps'],
         bins = 1000,
         color = '#f26c23',
         edgecolor = 'black',
         normed=True,
         label='Cohort 2017');
ax2.axvline(df_2017['ia_bandwidth_per_student_kbps'].median(),
            ymin=0, ymax=1, linestyle=':',
            label = str(round(df_2017['ia_bandwidth_per_student_kbps'].median(), 2))+ " kbps")
df_2017['ia_bandwidth_per_student_kbps'].plot(kind='kde', ax=ax2, color='darkcyan', label='');
ax2.legend();


# cohort 2018 plot: histogram, median vertical line, kde
ax3.hist(df_2018['ia_bandwidth_per_student_kbps'],
         bins = 1000,
         color = '#f9a677',
         edgecolor = 'black',
         normed=True,
        label='cohort 2018');
ax3.axvline(df_2018['ia_bandwidth_per_student_kbps'].median(),
            ymin=0, ymax=1, linestyle=':',
            label=str(round(df_2018['ia_bandwidth_per_student_kbps'].median(), 2))+ " kbps")
df_2018['ia_bandwidth_per_student_kbps'].plot(kind='kde', ax=ax3, color='darkcyan', label='');
ax3.legend();


# cohort 2019 plot: histogram, median vertical line, kde
ax4.hist(df_2019['ia_bandwidth_per_student_kbps'],
         bins = 1000,
         color = '#fbdbca',
         edgecolor = 'grey',
         normed=True,
         label='Cohort 2019'
        );
# median
ax4.axvline(df_2019['ia_bandwidth_per_student_kbps'].median(),
            ymin=0, ymax=1, linestyle=':',
            label=str(round(df_2019['ia_bandwidth_per_student_kbps'].median(), 2))+ " kbps")
df_2019['ia_bandwidth_per_student_kbps'].plot(kind='kde', ax=ax4, color='darkcyan',label='');
ax4.legend();

plt.suptitle("Histogram of Bandwidth Per Student for Cohort 2015, 2017, 2018, 2019");

# saving figure
os.chdir(GITHUB + '/''figure_images')
plt.savefig('id1021_histograms_bw_per_student_cohort_2018sots.png', bbox_inches = 'tight');
