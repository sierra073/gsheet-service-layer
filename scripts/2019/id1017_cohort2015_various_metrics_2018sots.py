'''
Sots 2019 Storyline

Within the first year of upgrade,
the median bandwidth per student percent increase was 75% and a median cost per mbps decrease of 36%.

'''

from __future__ import division
import os
import psycopg2
import numpy as np
import pandas as pd
from collections import defaultdict
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# credentials for frozen database
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

df_bw_cost_cols = ['district_id', 'funding_year',
                   'ia_bw_mbps_total','ia_bandwidth_per_student_kbps',
                   'ia_monthly_cost_total', 'ia_monthly_cost_per_mbps',
                   'meeting_2014_goal_no_oversub', 'meeting_2018_goal_oversub']

df_up_cols = ['district_id', 'funding_year', 'upgrade_indicator']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols).merge(df_up[df_up_cols],
                         on=merge_cols)

# change to numeric columns
numeric_cols = ['ia_bw_mbps_total','ia_bandwidth_per_student_kbps',
                   'ia_monthly_cost_total', 'ia_monthly_cost_per_mbps']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

# filter the dataframe with also fit for ia cost == True
df_filtered_both = df[(df.in_universe==True) &
                 (df.district_type=='Traditional') &
                 (df.fit_for_ia==True) & (df.fit_for_ia_cost==True)]



# looking only at districts with 4 years of clean data
df_clean_4yrs = df_filtered_both.groupby('district_id')['funding_year'].count().to_frame().reset_index()
df_clean_4yrs.columns = ['district_id', 'counts_funding_year']


# creating df of districts with 4 years clean data
id_clean_4yrs = df_clean_4yrs[df_clean_4yrs.counts_funding_year == 4].district_id.values
df_4years = df_filtered_both[df_filtered_both.district_id.isin(id_clean_4yrs)].sort_values(
                       ['district_id','funding_year'], ascending=[True, True])


# of the districts with clean 4 years, how many times did the district upgrade?
df_upgrade_counts = df_4years.groupby('district_id', as_index=False)['upgrade_indicator'].sum()
df_upgrade_counts.columns = ['district_id', 'num_times_upgraded']

# merge the dataframes
df_final = pd.merge(df_4years, df_upgrade_counts,  on='district_id')



# Defining cohorts
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
df_results.columns = ['district_id', 'year_first_meet2014', 'year_latest_upgrade', 'year_first_upgrade']


# changing 'district_id' from string to int
df_results['district_id'] = df_results['district_id'].apply(pd.to_numeric)

# adding new column: difference years before upgrade
df_results.loc[:, 'years_to_upgrade_after_meeting100'] = df_results['year_first_upgrade'] - df_results['year_first_meet2014']

# sort by district_id
df_results.sort_values('district_id', inplace=True)



# looking at cohort 2015
df_2015 = df_results[df_results.year_first_meet2014 == 2015]

# calculating different metrics for cohort 2015
df_2015_additional = pd.DataFrame()

for i, d_id in enumerate(df_2015.district_id.values):
    df_temp = df_final[df_final.district_id == d_id]
    year_first_meet2014 = df_2015[df_2015.district_id == d_id].year_first_meet2014.values[0]
    year_first_up = df_2015[df_2015.district_id == d_id].year_first_upgrade.values[0]
    year_latest_up = df_2015[df_2015.district_id == d_id].year_latest_upgrade.values[0]

    # find the bw per student in a given year
    bw_per_student_meeting100k = df_temp[df_temp.funding_year == year_first_meet2014].ia_bandwidth_per_student_kbps.values[0]
    bw_per_student_first_up = df_temp[df_temp.funding_year == year_first_up].ia_bandwidth_per_student_kbps.values[0]
    bw_per_student_latest_up = df_temp[df_temp.funding_year == year_latest_up].ia_bandwidth_per_student_kbps.values[0]

    # find the difference in bw
    difference_bw_first = bw_per_student_first_up - bw_per_student_meeting100k
    difference_bw_latest = bw_per_student_latest_up - bw_per_student_meeting100k

    # monthly cost, ia_monthly_cost_total
    mrc_meeting100k = df_temp[df_temp.funding_year == year_first_meet2014].ia_monthly_cost_total.values[0]
    mrc_first = df_temp[df_temp.funding_year == year_first_up].ia_monthly_cost_total.values[0]
    mrc_latest = df_temp[df_temp.funding_year == year_latest_up].ia_monthly_cost_total.values[0]

    # difference in monthly costs
    diff_mrc_first = mrc_first - mrc_meeting100k
    diff_mrc_latest = mrc_latest - mrc_meeting100k

    # cost per mbps, ia_monthly_cost_per_mbps
    costpm_meeting100k = df_temp[df_temp.funding_year == year_first_meet2014].ia_monthly_cost_per_mbps.values[0]
    costpm_first = df_temp[df_temp.funding_year == year_first_up].ia_monthly_cost_per_mbps.values[0]
    costpm_latest = df_temp[df_temp.funding_year == year_latest_up].ia_monthly_cost_per_mbps.values[0]

    # difference in cost per mbps
    diff_costpm_first = costpm_first - costpm_meeting100k
    diff_costpm_latest = costpm_latest - costpm_meeting100k

    # add district id
    df_2015_additional.loc[i, 'district_id'] = d_id
    # did the district meet the 1M goal after first upgrade? latest upgrade?
    bool_meet1M_at_2015 = df_temp[df_temp.funding_year == year_first_meet2014].meeting_2018_goal_oversub.values[0]
    bool_meet1M_first_up = df_temp[df_temp.funding_year == year_first_up].meeting_2018_goal_oversub.values[0]
    bool_meet1M_latest_up = df_temp[df_temp.funding_year == year_latest_up].meeting_2018_goal_oversub.values[0]
    df_2015_additional.loc[i, 'meet_100k_and_1M'] = bool_meet1M_at_2015
    df_2015_additional.loc[i, 'meet_1M_first_up'] = bool_meet1M_first_up
    df_2015_additional.loc[i, 'meet_1M_latest_up'] = bool_meet1M_latest_up

    # save results in a new columns
    # bw in specified years
    df_2015_additional.loc[i, 'bw_per_student_first_meet100k'] = bw_per_student_meeting100k
    df_2015_additional.loc[i, 'bw_per_student_first_upgrade'] = bw_per_student_first_up
    df_2015_additional.loc[i, 'bw_per_student_latest_upgrade'] = bw_per_student_latest_up
    # bandwidth
    df_2015_additional.loc[i, 'difference_bw_first'] = difference_bw_first
    df_2015_additional.loc[i, 'difference_bw_latest'] = difference_bw_latest
    df_2015_additional.loc[i, 'pcent_difference_bw_first'] = round(difference_bw_first/bw_per_student_meeting100k, 2)
    df_2015_additional.loc[i, 'pcent_difference_bw_latest'] = round(difference_bw_latest/bw_per_student_meeting100k, 2)
    # monthly cost total
    df_2015_additional.loc[i, 'difference_mrc_first'] = diff_mrc_first
    df_2015_additional.loc[i, 'difference_mrc_latest'] = diff_mrc_latest
    df_2015_additional.loc[i, 'pcent_difference_mrc_first'] = round(diff_mrc_first/mrc_meeting100k, 2)
    df_2015_additional.loc[i, 'pcent_difference_mrc_latest'] = round(diff_mrc_latest/mrc_meeting100k, 2)
    # cost per mbps
    df_2015_additional.loc[i, 'difference_costpm_first'] = diff_costpm_first
    df_2015_additional.loc[i, 'difference_costpm_latest'] = diff_costpm_latest
    df_2015_additional.loc[i, 'pcent_difference_costpm_first'] = round(diff_costpm_first/costpm_meeting100k, 2)
    df_2015_additional.loc[i, 'pcent_difference_costpm_latest'] = round(diff_costpm_latest/costpm_meeting100k, 2)

# concatenate the two dataframes together
df_2015_extended = pd.merge(df_2015, df_2015_additional, on='district_id')

# summary of results
d_summary = {'cohort_year': 2015,
'bw_per_student_100k_to_first_median': round(df_2015_extended.difference_bw_first.median(), 2),
'bw_per_student_100k_to_latest_median': round(df_2015_extended.difference_bw_latest.median(), 2),
'bw_per_student_100k_to_first_pcent_diff_median': round(df_2015_extended.pcent_difference_bw_first.median(), 2),
'bw_per_student_100k_to_latest_pcent_diff_median': round(df_2015_extended.pcent_difference_bw_latest.median(), 2),
'monthly_cost_diff_100k_first': round(df_2015_extended.difference_mrc_first.median(), 2),
'monthly_cost_diff_10p0k_latest': round(df_2015_extended.difference_mrc_latest.median(), 2),
'monthly_cost_pcent_diff_100k_first': round(df_2015_extended.pcent_difference_mrc_first.median(), 2),
'monthly_cost_pcent_diff_100k_latest': round(df_2015_extended.pcent_difference_mrc_latest.median(), 2),
'cost_per_mbps_diff_100k_to_first': round(df_2015_extended.difference_costpm_first.median(), 2),
'cost_per_mbps_diff_100k_to_latest': round(df_2015_extended.difference_costpm_latest.median(), 2),
'cost_per_mbps_pcent_diff_100k_first': round(df_2015_extended.pcent_difference_costpm_first.median(), 2),
'cost_per_mbps_pcent_diff_100k_latest': round(df_2015_extended.pcent_difference_costpm_latest.median(), 2)}
df_summary = pd.DataFrame(d_summary,index=[0])

# save as a csv
os.chdir(GITHUB + '/Projects/sots-isl/data')
df_summary.to_csv('id1017_cohort2015_various_metrics_2018sots.csv', index=False)
