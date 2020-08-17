#!/usr/bin/env python
# coding: utf-8

# ### Insight Follow Ups

# District version of charts
#
#
# Original Follow Up
#
# - Could we do what % of districts not meeting would reach 1 Mbps with a 100%, 200%, 300%, 400% increase, cumulative?
# - For example, if everyone went up 500%, what would happen?


import numpy as np
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
PORT_DAR = os.environ.get("PORT_DAR")
GITHUB = os.environ.get("GITHUB")

#open connection to DB
myConnection = psycopg2.connect( host=HOST_DAR,
                                user=USER_DAR,
                                password=PASSWORD_DAR,
                                database=DB_DAR,
                                port=PORT_DAR)

sql_query = """
select
    d.district_id,
    d.funding_year,
    d.district_type,
    d.in_universe,
    dffa.fit_for_ia,
    d.num_students,
    dbw.ia_bw_mbps_total,
    dbw.projected_bw_fy2018,
    dbw.projected_bw_fy2018_cck12,
    dbw.meeting_2018_goal_oversub

-- basic district info
FROM ps.districts d

-- district costs and bw
JOIN ps.districts_bw_cost dbw
ON d.district_id = dbw.district_id
AND d.funding_year = dbw.funding_year

-- to check for fit for ia
JOIN ps.districts_fit_for_analysis dffa
ON d.district_id = dffa.district_id
AND d.funding_year = dffa.funding_year

where d.funding_year = 2019
"""

#query data
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=names)

# remove any printing of error
pd.options.mode.chained_assignment = None

# Masks/filters
mask_not_meeting = df.meeting_2018_goal_oversub == False
mask_meeting = df.meeting_2018_goal_oversub == True
mask_traditional = df.district_type == 'Traditional'
mask_fit_ia = df.fit_for_ia == True
mask_in_universe = df.in_universe == True


# Convert Decimal to Float
numeric_cols = ['num_students', 'ia_bw_mbps_total','projected_bw_fy2018']
df[numeric_cols] = df[numeric_cols].astype(float)


# Extrapolation
num_population_districts = df[mask_in_universe & mask_traditional].district_id.nunique()
num_sample_districts = df[mask_fit_ia &
                          mask_in_universe &
                          mask_traditional].district_id.nunique()

# uncomment if you want to print out
# print(f"Number of population districts: {num_population_districts}")
# print(f"Number of sample districts: {num_sample_districts}")

num_population_students = df[mask_in_universe & mask_traditional].num_students.sum()
num_sample_students = df[mask_fit_ia &
                          mask_in_universe &
                          mask_traditional].num_students.sum()

# print(f"Number of population students: {num_population_students}")
# print(f"Number of sample students: {num_sample_students}")

# districts clean and not meeting 1 mbps
df_clean = df[mask_not_meeting &
                mask_traditional &
                mask_fit_ia &
                mask_in_universe]

# Calculating pct_bw_increase_to_meet_1Mbps
df_clean.loc[:, 'pct_bw_increase_to_meet_1Mbps'] = ((df_clean.loc[:, 'projected_bw_fy2018'] - df_clean.loc[: ,'ia_bw_mbps_total'])/df_clean.loc[:, 'ia_bw_mbps_total'])*100

# Place `pct_bw_increase_to_meet_1Mbps` into bins for plotting
# indicate bins for categories
max_bin = 3100
increment = 100
bins = np.arange(0, max_bin, increment)

# create labels
labels = []
for i, b in enumerate(bins):
    if i == 0:
        labels.append("less than " + str(bins[i+1]) + '%')
    elif i < len(bins)-1:
        labels.append(str(bins[i])+'%')
    else:
        break

# add final category to labels
labels.append(str(max_bin-increment) +'% or over')

# categorize bw_pct_change
df_clean.loc[:,'bw_pct_change_category'] = pd.cut(df_clean['pct_bw_increase_to_meet_1Mbps'],
                                                     bins=bins,
                                                     labels=labels[:-1], right=False)

# add new category
df_clean.loc[:, 'bw_pct_change_category'] = df_clean['bw_pct_change_category'].cat.add_categories(str(max_bin-increment) + '% or over')

# fill in Nans (anything over max_bin)
df_clean[['bw_pct_change_category']] = df_clean[['bw_pct_change_category']].fillna(value=str(max_bin-increment) +'% or over')


# change index to string, and count values for plotting
df_clean.loc[:, 'bw_pct_change_category'] = df_clean['bw_pct_change_category'].astype(str)

# converting list to df for merging
df_temp = pd.DataFrame({'labels': labels})
df_temp.set_index('labels', inplace=True)

# count instances of category for district count
df_pct_change = df_clean.bw_pct_change_category.value_counts().to_frame()

# add student counts
df_pct_change = pd.merge(df_pct_change,
                         pd.DataFrame(df_clean.groupby('bw_pct_change_category')['num_students'].sum()),
                         left_index=True,
                         right_index=True)

# merge into resultant dataframe to keep order of index
df_pct_change = df_temp.merge(df_pct_change, left_index=True, right_index=True )
df_pct_change.reset_index(inplace=True)

# rename index column
df_pct_change.columns = ['pct_category', 'district_count', 'num_students']

# add extrapolated numbers
df_pct_change['district_count_extrapolated'] = (df_pct_change['district_count']*num_population_districts)/(num_sample_districts)

# add extrapolated numbers for student counts
df_pct_change['num_students_extrapolated'] = (df_pct_change['num_students']*num_population_students)/(num_sample_students)

# Adding cumulative counts

# cumulative sum (sample)
df_pct_change.loc[:, 'district_count_cumsum'] = df_pct_change['district_count'].cumsum(axis = 0)

# cumulative sum (extrapolated)
df_pct_change.loc[:, 'district_count_extrapolated_cumsum'] = df_pct_change['district_count_extrapolated'].cumsum(axis = 0)

# percentage by pct_category - cumulative(sample)
df_pct_change.loc[:, 'district_count_cumsum_pct'] = df_pct_change.loc[:, 'district_count_cumsum']/df_pct_change.district_count.sum()

# percentage by pct_category - cumulative(extrapolated)
df_pct_change.loc[:, 'district_count_extrapolated_cumsum_pct'] = df_pct_change.loc[:, 'district_count_extrapolated_cumsum']/df_pct_change.district_count_extrapolated.sum()

# percentage by pct_category (sample)
df_pct_change.loc[:, 'district_count_pct'] = df_pct_change.loc[:, 'district_count']/df_pct_change.district_count.sum()

# cumulative student counts (sample)
df_pct_change.loc[:, 'num_students_cumsum'] = df_pct_change['num_students'].cumsum(axis = 0)

# cumulative sum (extrapolated)
df_pct_change.loc[:, 'num_students_extrapolated_cumsum'] = df_pct_change['num_students_extrapolated'].cumsum(axis = 0)

# percentage by pct_category - cumulative(sample)
df_pct_change.loc[:, 'num_students_cumsum_pct'] = df_pct_change.loc[:, 'num_students_cumsum']/df_pct_change.num_students.sum()

# percentage by pct_category - cumulative(extrapolated)
df_pct_change.loc[:, 'num_students_extrapolated_cumsum_pct'] = df_pct_change.loc[:, 'num_students_extrapolated_cumsum']/df_pct_change.num_students_extrapolated.sum()

# percentage by pct_category (sample)
df_pct_change.loc[:, 'num_students_pct'] = df_pct_change.loc[:, 'num_students']/df_pct_change.num_students.sum()

# pretty print of 'num_students_cumsum'
df_pct_change.loc[:, 'num_students_cumsum_pp'] = (df_pct_change.loc[:, 'num_students_cumsum']/1000000).apply(lambda x: '{:,.1f}M'.format(x))

# pretty print of 'num_students'
df_pct_change.loc[:, 'num_students_pp'] = (df_pct_change.loc[:, 'num_students']/1000000).apply(lambda x: '{:,.1f}M'.format(x))


# Cumulative Plot - District Version

# plotting the figure
plt.figure(figsize=(15, 7))
xmarks = np.arange(0, df_pct_change.shape[0])
plt.plot(xmarks, df_pct_change['district_count_cumsum'], color='#009296', marker='o')
plt.fill_between(xmarks,df_pct_change['district_count_cumsum'], color='#009296', alpha=0.3)

# ticks, titles, axis labels
# plt.ylabel("Percent/Number of Districts Not Meeting 1 Mbps")
plt.title("Percent/Number of Districts Not Meeting 1 Mbps and the Percent Bandwidth Increase Needed to Meet 1 Mbps", y=1.18)
plt.xticks(xmarks, df_pct_change.pct_category.values, rotation=20)
plt.box(on=None)
plt.yticks([])

# add sum labels
for x0, v0, label, pct_label in zip(xmarks,
                         df_pct_change['district_count_cumsum'],
                         df_pct_change['district_count_cumsum'],
                         df_pct_change['district_count_cumsum_pct']
                        ):
    plt.text(x0, v0+100, round(label), ha='center', va='bottom', color='orange')
    plt.text(x0, v0+330, str(round(pct_label*100, 1))+'%', ha='center', va='bottom')

plt.text(1, 9000, "Median: "+str(round(df_clean.pct_bw_increase_to_meet_1Mbps.median(), 1)), ha='center', va='bottom')
plt.text(1, 8700, "Mean: "+str(round(df_clean.pct_bw_increase_to_meet_1Mbps.mean(), 1)), ha='center', va='bottom');

# save to isl folder figure_images
os.chdir(GITHUB + '/Projects/sots-isl/figure_images/')
plt.savefig("id6002_districts_not_meeting_by_pct_bw_increase_districts.png", bbox_inches = 'tight')
