#!/usr/bin/env python
# coding: utf-8

# ### As of 9/3/2019
#
# Updated pivotal card: https://www.pivotaltracker.com/story/show/168254684
#
# Updates:
# - Updated states: AL, AR, CT, DE, GA (use total IA Spend), ME, NC, ND, NE, RI, SC, SD, UT, WA, WY, WV
# - update current upstream spend to be overall IA spend
# - update projected spend to include ISP cost projection
# - include the following metrics from the draft:
#     -xx million students on state networks can be quickly upgraded to the 1 Mbps per student goal
#     -This will upgrade over ## million students and position the nation to connect 99% of our schools to the 1 Mbps per student goal by 202
# - Chart #: State leadership can make digital learning a reality for ## million students by 2022
# - [Insert 2 national heat maps showing where states are on 1 Mbps (% of districts or students) in 2019 and in 2022 if all of the above upgrades happen]
#
# This code runs at currently these prices:
# projected_price_1g = 1100
# projected_price_10g = 2200



import math
import numpy as np
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# get states that are in state network
HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
PORT_DAR = os.environ.get("PORT_DAR")
GITHUB = os.environ.get("GITHUB")

#open connection to DB
myConnection = psycopg2.connect(host=HOST_DAR,
                                user=USER_DAR,
                                password=PASSWORD_DAR,
                                database=DB_DAR)

# sql_query_part1
sql_query = """
select
  d.district_id,
  d.funding_year,
  d.in_universe,
  d.district_type,
  d.name,
  d.state_code,
  d.num_students,
  d.num_schools,
  dbw.ia_bw_mbps_total,
  dbw.ia_monthly_cost_total,
  dbw.meeting_2018_goal_no_oversub,
  dbw.meeting_2018_goal_oversub,
  dffa.fit_for_ia,
  dffa.fit_for_ia_cost
from
  ps.districts d
  JOIN ps.districts_bw_cost dbw on d.funding_year = dbw.funding_year
  and d.district_id = dbw.district_id
  join ps.districts_fit_for_analysis dffa ON d.funding_year = dffa.funding_year
  and d.district_id = dffa.district_id
  join ps.states_static ss on d.state_code = ss.name
where
  d.funding_year = 2019
  and ss.state_network_natl_analysis = true
"""

#pull bandwidths from DB
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=names)

# convert from decimal to numeric
numeric_cols = ['num_students', 'num_schools','ia_monthly_cost_total']
df[numeric_cols] = df[numeric_cols].astype(float)


# ### Masks/Filters
mask_less_than_1k_students = df.num_students <= 1000
mask_1k_to_10k = (df.num_students > 1000) & (df.num_students <= 10000)
mask_10k_plus = df.num_students > 10000
mask_fit_ia = df.fit_for_ia == True
mask_fit_cost = df.fit_for_ia_cost == True
mask_in_universe = df.in_universe == True
mask_traditional = df.district_type == 'Traditional'


# ### Add Multiple of 10G's Needed to meet 1 Mbps Goal
# districts with 10k plus students
sub_cols = ['district_id', 'funding_year', 'name', 'state_code','num_students', 'num_schools',
            'ia_bw_mbps_total', 'meeting_2018_goal_oversub']
df_10k_plus = df[sub_cols][mask_10k_plus & mask_in_universe & mask_traditional]

# change the price as needed
# projected_price_1g = 890
# projected_price_10g = 2500

# projected_price_1g = 1212
# projected_price_10g = 2415

# # evan request 9/17 12pm
projected_price_1g = 1100
projected_price_10g = 2200

# how many multiples of 10G to get them to their goals? with oversubscription
def round_up_nearest_10k(row):
    return int(math.ceil(row/10000))

# without oversubscription
df_10k_plus['multiple_10G_no_oversub'] = df_10k_plus.num_students.apply(round_up_nearest_10k)

# projected cost to get 10k plus to meet 1 Mbps without oversub by district
df_10k_plus['projected_mrc_10g_10kplus_no_oversub'] = df_10k_plus['multiple_10G_no_oversub']*projected_price_10g


# ### Add Projected Costs for 1G and 10G
df_subset = df[mask_in_universe & mask_traditional][['district_id', 'funding_year', 'name', 'state_code', 'fit_for_ia', 'fit_for_ia_cost','num_students', 'num_schools',
       'ia_bw_mbps_total', 'ia_monthly_cost_total', 'meeting_2018_goal_oversub']]


def projected_cost_1G(row):
    if row <= 1000:
        return projected_price_1g

def projected_cost_10G(row):
    if (row > 1000) and (row <= 10000):
        return projected_price_10g

# new mrc with $890 1G projected price
df_subset['projected_mrc_1g'] = df_subset.num_students.apply(projected_cost_1G)

# new mrc with $2500 per 10G projected price
df_subset['projected_mrc_10g'] = df_subset.num_students.apply(projected_cost_10G)


# ### Add Projected Costs for 10G plus
# merge with df_ga_ne_cost
sub_cols = ['district_id', 'multiple_10G_no_oversub',  'projected_mrc_10g_10kplus_no_oversub']
df_result = pd.merge(df_subset, df_10k_plus[sub_cols], how='left', on=['district_id'])

# fill in nans with zeroes
df_result.fillna(0, inplace=True)


# ### Add Projected ISP Costs and Total Projected Costs
# Projected ISP Costs: num_students * 0.170 Mbps/student * $/mbps projected in 2023

concurrecy_factor = 0.170
costs_per_mbps_isp_2023 = [0.30, 0.49, 0.74]

for cpm in costs_per_mbps_isp_2023:
    # total projected ISP cost
    df_result["projected_isp_"+str(cpm)[2:]] = df_result['num_students']*concurrecy_factor*cpm
    # total projected cost is the sum of 1g, 10g and 10gplus
    df_result['total_projected_cost'+str(cpm)[2:]] = df_result['projected_mrc_1g'] +                                                      df_result['projected_mrc_10g'] +                                                      df_result['projected_mrc_10g_10kplus_no_oversub'] +                                                      df_result["projected_isp_"+str(cpm)[2:]]



# ### Adding Extrapolated IA Costs for dirty cost districts
# series average cost by state
s_avg_ia_cost = df[mask_fit_cost].groupby('state_code')['ia_monthly_cost_total'].mean()

# function to extrapolate ia cost to dirty districts
def extrapolate_ia_cost(row):
    if row['fit_for_ia_cost'] == False:
        return s_avg_ia_cost[row['state_code']]
    else:
        return row['ia_monthly_cost_total']


df_result.loc[:, 'ia_monthly_cost_total_extrap'] = df_result[['state_code',
                                                              'fit_for_ia_cost',
                                                              'ia_monthly_cost_total']].apply(extrapolate_ia_cost, axis=1)


# ### Save detailed list to csv

# rearrange df_result columns
df_result = df_result[['district_id', 'funding_year', 'name', 'state_code', 'fit_for_ia',
       'fit_for_ia_cost', 'num_students', 'num_schools', 'ia_bw_mbps_total',
       'ia_monthly_cost_total', 'ia_monthly_cost_total_extrap', 'meeting_2018_goal_oversub',
       'projected_mrc_1g', 'projected_mrc_10g', 'multiple_10G_no_oversub',
       'projected_mrc_10g_10kplus_no_oversub',
       'projected_isp_3','projected_isp_49', 'projected_isp_74',
       'total_projected_cost3', 'total_projected_cost49', 'total_projected_cost74']]

# sort by state and district name
df_result.sort_values(['state_code', 'name'], inplace=True)

# save as csv
os.chdir(GITHUB + '/''data/')
df_result.to_csv('id6012_state_network_cost_upstream_isp_detailed_list.csv', index=False)


# # ### Aggregating by State (Rerun here for df_agg)
#
# # aggregate ia_monthly_cost_total and total_projected_cost by state
# df_agg = df_result.groupby('state_code').agg({'district_id': 'nunique',
#                                               'ia_monthly_cost_total': 'sum',
#                                               'num_students': 'sum',
#                                               'num_schools': 'sum',
#                                               'total_projected_cost3': 'sum',
#                                               'total_projected_cost49': 'sum',
#                                               'total_projected_cost74': 'sum'
#                                              })
#
# # add already spending boolean column
# df_agg['already_spending_3'] = df_agg['ia_monthly_cost_total'] >= df_agg['total_projected_cost3']
# df_agg['already_spending_49'] = df_agg['ia_monthly_cost_total'] >= df_agg['total_projected_cost49']
# df_agg['already_spending_74'] = df_agg['ia_monthly_cost_total'] >= df_agg['total_projected_cost74']
#
#
# # reset index
# df_agg.reset_index(inplace=True)
#
# # rename columns
# df_agg.columns = ['state_code', 'district_counts', 'ia_monthly_cost_total', 'num_students',
#        'num_schools', 'total_projected_cost3', 'total_projected_cost49',
#        'total_projected_cost74', 'already_spending_3', 'already_spending_49',
#        'already_spending_74']
#
#
# # ### Add Number of Districts Not Meeting and Number of Students Not Meeting
# # reran with fix: removed fit_for_ia_cost, and changed meeting_2018_goal_no_oversub to oversub
#
# # district count not meeting 1 Mbps
# df_dist_count_not_meeting = df_result[(df_result.fit_for_ia == True) &
#                                       (df_result.meeting_2018_goal_oversub == False)].groupby('state_code')['district_id'].nunique().to_frame().reset_index()
# df_dist_count_not_meeting.columns = ['state_code', 'num_districts_not_meeting']
#
# # student count not meeting 1 Mbps
# df_stud_count_not_meeting = df_result[(df_result.fit_for_ia == True) &
#                                       (df_result.meeting_2018_goal_oversub == False)].groupby('state_code')['num_students'].sum().to_frame().reset_index()
# df_stud_count_not_meeting.columns = ['state_code', 'num_students_not_meeting']
#
# # school count not meeting 1 Mbps
# df_school_count_not_meeting = df_result[(df_result.fit_for_ia == True) &
#                                         (df_result.meeting_2018_goal_oversub == False)].groupby('state_code')['num_schools'].sum().to_frame().reset_index()
# df_school_count_not_meeting.columns = ['state_code', 'num_schools_not_meeting']
#
#
# # ### Add Extrapolated Districts Newly Meeting
# # take population and sample
# s_population_districts = df[(df.in_universe == True) &
#                            (df['district_type']=='Traditional')].groupby('state_code').district_id.nunique()
#
#
# # rerun: removed fit_for_ia_cost
# s_sample_districts = df[(df.fit_for_ia == True) &
#                          (df.in_universe == True) &
#                          (df['district_type']=='Traditional')].groupby('state_code').district_id.nunique()
#
# # function to extrapolate ia cost to dirty districts
# def extrapolate_districts(row):
#     return (row['num_districts_not_meeting']*s_population_districts[row['state_code']])/s_sample_districts[row['state_code']]
#
# # total number of students (meeting and not meeting)
# df_dist_count_not_meeting.loc[:, 'num_districts_newly_meeting_extrap'] = df_dist_count_not_meeting[['state_code',
#                                                                                                     'num_districts_not_meeting']].apply(extrapolate_districts, axis=1)
#
# # ### Add extrapolated Students Newly Meeting
#
# # take population and sample
# s_population_students = df[(df.in_universe == True) & (df.district_type == 'Traditional')].groupby('state_code').num_students.sum()
#
# s_sample_students = df[(df.fit_for_ia == True) &
#                          (df.in_universe == True) &
#                          (df['district_type']=='Traditional')].groupby('state_code').num_students.sum()
#
# # function to extrapolate ia cost to dirty districts
# def extrapolate_students(row):
#     return (row['num_students_not_meeting']*s_population_students[row['state_code']])/s_sample_students[row['state_code']]
#
# # total number of students (meeting and not meeting)
# df_stud_count_not_meeting.loc[:, 'num_students_newly_meeting_extrap'] = df_stud_count_not_meeting[['state_code',
#                                                                                                'num_students_not_meeting']].apply(extrapolate_students, axis=1)
#
# # ### Add Extrapolated Number of Schools Not Meeting
#
# # take population and sample
# s_population_schools = df[(df.in_universe == True) &
#                           (df['district_type']=='Traditional')].groupby('state_code').num_schools.sum()
#
# s_sample_schools = df[(df.fit_for_ia == True) &
#                       (df.in_universe == True) &
#                       (df['district_type']=='Traditional')].groupby('state_code').num_schools.sum()
#
# # function to extrapolate ia cost to dirty districts
# def extrapolate_schools(row):
#     return (row['num_schools_not_meeting']*s_population_schools[row['state_code']])/s_sample_schools[row['state_code']]
#
# # extrapolated number of schools not meeting
# df_school_count_not_meeting.loc[:, 'num_schools_newly_meeting_extrap'] = df_school_count_not_meeting[['state_code',
#                                                                                                       'num_schools_not_meeting']].apply(extrapolate_schools, axis=1)
#
# # ### Merge into df_agg
# df_agg = df_agg.merge(df_dist_count_not_meeting,
#                       on='state_code').merge(df_stud_count_not_meeting,
#                                              on='state_code').merge(df_school_count_not_meeting,
#                                                                     on='state_code')
#
# # rearrange columns
# df_agg = df_agg[['state_code', 'district_counts', 'num_districts_not_meeting', 'num_districts_newly_meeting_extrap',
#                  'num_students', 'num_students_not_meeting', 'num_students_newly_meeting_extrap',
#                  'num_schools', 'num_schools_not_meeting', 'num_schools_newly_meeting_extrap',
#                  'ia_monthly_cost_total',
#                  'total_projected_cost3', 'total_projected_cost49', 'total_projected_cost74',
#                  'already_spending_3', 'already_spending_49', 'already_spending_74']]
#
# # ### Adding Pretty Print
#
# # format thousands as string
# cols_to_pp = ['num_students', 'num_students_not_meeting', 'num_students_newly_meeting_extrap',
#               'ia_monthly_cost_total',
#               'total_projected_cost3', 'total_projected_cost49','total_projected_cost74']
#
# def format_k_MM(row):
#     if row >= 1000000:
#         row_to_format = row/1000000
#         return '{:,.1f}M'.format(row_to_format)
#     else:
#         row_to_format = row/1000
#         return '{:,.0f}k'.format(row_to_format)
#
# for col in cols_to_pp:
#     df_agg[col+'_pp'] = df_agg[col].apply(format_k_MM)
#
# df_agg[df_agg.state_code.isin(['CT', 'ME'])]['num_students_newly_meeting_extrap'].sum()
#
#
# # save aggregation to csv for qa
# os.chdir(GITHUB + '/''data/')
# df_agg.to_csv("id6015_state_network_cost_upstream_isp_aggregate_by_state.csv", index=False)


# # ### Plot: Current Total IA Spending vs. Total Projected Cost (ISP Cost included)
#
# # inputs
# sort_col = 'total_projected_cost3'
# compare_col = 'ia_monthly_cost_total'
# already_spending_col = 'already_spending_3'
# color_code = '03'
# df_input = df_agg.sort_values([already_spending_col, sort_col])
#
# # customize colors
# bar_colors = {'03': '#c44f27', '49': '#006b6e', '74':'#68ab44'}
# colors = []
# for proj_cost, orig_cost in zip(df_input[sort_col], df_input[compare_col]):
#     if orig_cost >= proj_cost:
#         # change bar color
#         colors.append(bar_colors[color_code])
#     else:
#         colors.append('#f9d2a3')
#
# fig, ax = plt.subplots(figsize=(17, 17))
#
# bar_size = 0.4
#
# y_locs = np.arange(df_input.shape[0])
#
# rects1 = ax.barh(y_locs, df_input[sort_col], align='edge', edgecolor='white', height=bar_size, color=colors, label="Projected Cost")
# rects2 = ax.barh(y_locs - bar_size, df_input[compare_col], edgecolor='white', align='edge', height=bar_size, color='grey', label="Current IA Spending", alpha=0.5)
#
# # pretty plot
# plt.yticks(y_locs, df_input.state_code);
# for ticklabel, tickcolor in zip(plt.gca().get_yticklabels(), colors):
#     ticklabel.set_color('grey')
#     ticklabel.set_fontsize(12)
#     if tickcolor == bar_colors[color_code]:
#         ticklabel.set_color(tickcolor)
#         ticklabel.set_fontweight('bold')
#
#
# # states already spending
# states_already_spending = df_input[df_input[already_spending_col] == True]['state_code'].unique()
#
# # total students newly meeting
# total_students_newly_meeting_pp = '{:,.1f}M'.format(df_input[df_input.state_code.isin(states_already_spending)]['num_students_newly_meeting_extrap'].sum()/1000000)
#
# # total district newly meeting
# total_districts_newly_meeting_pp = round(int(df_input[df_input.state_code.isin(states_already_spending)]['num_districts_newly_meeting_extrap'].sum()))
#
# # total schools newly meeting
# total_schools_newly_meeting_pp = round(int(df_input[df_input.state_code.isin(states_already_spending)]['num_schools_newly_meeting_extrap'].sum()))
#
#
#
#
# plt.title("Current State Network Spending (Total IA) vs. Projected Cost (Upstream Cost + ISP Cost Included at $0.30/Mbps)", fontsize=14)
# plt.tick_params(top=False, bottom=False, left=False, right=False, labelleft=True, labelbottom=False)
# plt.box(None)
# plt.legend(loc='upper right');
#
# # add labels
# for y0, y1, v0, v1, state, label_proj, label_sn, label_students_nm, label_students in zip(y_locs, y_locs - bar_size,
#                                             df_input[sort_col], df_input[compare_col],
#                                                 df_input.state_code,
#                                                 df_input[sort_col+'_pp'], df_input[compare_col+'_pp'],
#                                                 df_input['num_students_newly_meeting_extrap_pp'],
#                                                 df_input['num_students_pp']
#                                                ):
#     if state in states_already_spending:
#         plt.text(v0, y0, label_proj + ", " + label_students_nm + " students newly meeting (out of " +                  label_students + " total)" , ha='left', va='bottom')
#         plt.text(v1, y1, label_sn, ha='left', va='bottom')
#     else:
#         plt.text(v0, y0, label_proj + " (" + label_students + " total students)" , ha='left', va='bottom')
#         plt.text(v1, y1, label_sn, ha='left', va='bottom')
#
#
# # add conclusion - box
# plt.text(0.6*max(df_input[compare_col]), 7.8, "Results: ", fontsize=14)
# plt.text(0.6*max(df_input[compare_col]), 7.4, str(df_input[already_spending_col].sum()) + " (out of " + str(df_input.shape[0]) + ") state networks already spending" , fontsize=14, fontweight='bold', color=bar_colors[color_code])
#
# plt.text(0.6*max(df_input[compare_col]), 7., total_students_newly_meeting_pp + " students newly meeting                        ", fontsize=14, fontweight='bold', color=bar_colors[color_code], bbox=dict(facecolor='none', edgecolor='black', boxstyle='round,pad=3.9'));
#
# plt.text(0.6*max(df_input[compare_col]), 6.6, str(total_districts_newly_meeting_pp) + " districts newly meeting", fontsize=14, fontweight='bold', color=bar_colors[color_code])
#
# plt.text(0.6*max(df_input[compare_col]), 6.2, str(total_schools_newly_meeting_pp) + " schools newly meeting", fontsize=14, fontweight='bold', color=bar_colors[color_code]);
#
#
#
# # save figure
# plt.savefig("projected_cost_with_isp_cpm3_092319_no_oversub.png", bbox_inches='tight')
