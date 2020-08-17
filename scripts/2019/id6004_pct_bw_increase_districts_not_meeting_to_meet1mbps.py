#!/usr/bin/env python
# coding: utf-8

# ### Circuit Size Tipping Point
#
# Districts Not Meeting 1Mbps in 2019 and the percent change needed to get to 1Mbps per student
#
# #### What:
# - Add more intervals after 500% since there are so many
# - Create a version with percentages
#
# Using `projected_bw_fy2018` uses the concurrency factor for Medium/Larges/Megas in calculating projected bw needed to meet 1 Mbps
# Author: Kat Aquino


from __future__ import division
import numpy as np
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import datetime


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

# sql query
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
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=names)

# suppressing SettingWithCopyWarning
pd.options.mode.chained_assignment = None

#  Masks/Filters
mask_not_meeting = df.meeting_2018_goal_oversub == False
mask_meeting = df.meeting_2018_goal_oversub == True
mask_traditional = df.district_type == 'Traditional'
mask_fit_ia = df.fit_for_ia == True
mask_in_universe = df.in_universe == True


# Convert Decimal to Float
numeric_cols = ['num_students', 'ia_bw_mbps_total','projected_bw_fy2018',]
df[numeric_cols] = df[numeric_cols].astype(float)


# Extrapolation
num_population_districts = df[mask_in_universe & mask_traditional].district_id.nunique()
num_sample_districts = df[mask_fit_ia &
                          mask_in_universe &
                          mask_traditional].district_id.nunique()

# print(f"Number of population districts: {num_population_districts}")
# print(f"Number of sample districts: {num_sample_districts}")

# districts not meeting 1 mbps
df_fit_ia = df[mask_not_meeting &
                mask_traditional &
                mask_fit_ia &
                mask_in_universe]

# Calculating Bandwidth Percent Change

# add 'bw_pct_change'
df_fit_ia.loc[:, 'bw_pct_change_to_meet_2018_goal'] = ((df_fit_ia['projected_bw_fy2018'] - df_fit_ia['ia_bw_mbps_total'])/df_fit_ia['ia_bw_mbps_total'])*100

# chart colors
locale_colors = {'Rural': '#fac4a5', 'Town':'#f26c23', 'Suburban':'#c44f27', 'Urban':'#83351a'}


# Number of Districts, Percent Bandwidth Increase, Overall: Slide 37

# indicate bins for categories
max_bin = 1050
increment = 50
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
df_fit_ia.loc[:,'bw_pct_change_category'] = pd.cut(df_fit_ia['bw_pct_change_to_meet_2018_goal'],
                                                     bins=bins,
                                                     labels=labels[:-1], right=False)

# add new category
df_fit_ia.loc[:, 'bw_pct_change_category'] = df_fit_ia['bw_pct_change_category'].cat.add_categories(str(max_bin-increment) + '% or over')

# fill in Nans (anything over max_bin)
df_fit_ia[['bw_pct_change_category']] = df_fit_ia[['bw_pct_change_category']].fillna(value=str(max_bin-increment) +'% or over')


# change index to string, and count values for plotting
df_fit_ia.loc[:, 'bw_pct_change_category'] = df_fit_ia['bw_pct_change_category'].astype(str)

# converting list to df for merging
df_temp = pd.DataFrame({'labels': labels})
df_temp.set_index('labels', inplace=True)

# count instances of category for district count
df_pct_change = df_fit_ia.bw_pct_change_category.value_counts().to_frame()

# merge into resultant dataframe to keep order of index
df_pct_change = df_temp.merge(df_pct_change, left_index=True, right_index=True )
df_pct_change.reset_index(inplace=True)

# rename index column
df_pct_change.columns = ['pct_category', 'district_count']

# add extrapolated numbers
df_pct_change['district_count_extrapolated'] = (df_pct_change['district_count']*num_population_districts)/(num_sample_districts)

# add percentage column
df_pct_change['pct_by_category'] = df_pct_change['district_count_extrapolated']/df_pct_change['district_count_extrapolated'].sum()

# plotting the figure
plt.figure(figsize=(15, 7))
xmarks = np.arange(0, df_pct_change.shape[0])
plt.bar(xmarks, df_pct_change['district_count_extrapolated'], color='#009296')

# ticks, titles, axis labels
plt.ylabel("Number of Districts")
plt.xticks(xmarks, df_pct_change.pct_category.values, rotation=20)
plt.box(on=None)
plt.yticks([])

# add sum labels
for x0, v0, label, pct_label in zip(xmarks,
                         df_pct_change['district_count_extrapolated'],
                         df_pct_change['district_count_extrapolated'],
                         df_pct_change['pct_by_category']
                        ):
    plt.text(x0, v0, round(label), ha='center', va='bottom', color='orange')
    plt.text(x0, v0+60, str(round(pct_label*100, 1))+'%', ha='center', va='bottom')

plt.text(15, 1800, "Percent Bandwidth Increase ", ha='center', va='bottom', fontsize=12)
plt.text(16, 1700, "Median: "+str(round(df_fit_ia.bw_pct_change_to_meet_2018_goal.median(), 1)), ha='center', va='bottom')
plt.text(16, 1600, "Mean: "+str(round(df_fit_ia.bw_pct_change_to_meet_2018_goal.mean(), 1)), ha='center', va='bottom');


# save to isl folder figure_images
os.chdir(GITHUB + '/Projects/sots-isl/figure_images/')
plt.savefig("id6004_pct_bw_increase_districts_not_meeting_to_meet1mbps.png", bbox_inches = 'tight')
