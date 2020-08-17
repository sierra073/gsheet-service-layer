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
#
# ### Original:
#
# ideally, evan would review the material and give us the price projection he is comfortable with (he has been asking us for many pieces of analysis to lead to this question, including molly/surafael/jamies projections and now this one from adam). in the absence of his final 10G and 1G projection, i have recommended using \$ 2500 for 10G and \$ 890 for 1G.
#
# How:
# first, please review the logic for my projection. i tried to make it something that would appeal to evan. it is located here: https://docs.google.com/document/d/1R12jdZjNvhG9_4yExXCCIozwX6kZ4Axcs4dXt0y3Efk/edit?disco=AAAADU2fU3g
#
#
# then calculate, for all state network states, how much would it cost to get the state 1G for all of their districts with <1,000 students, 10G for 1,000-10,000 students? for districts over 10,000 students, however many multiples of 10G get them to their goals? (evan has not requested adjustment for oversubscription, but an alternative we can try if these numbers are low are oversubscription adjustments). note that these projections may change, so it would be wise to make the cost for 1G and 10G easy to malipulate?
#
#
# ### New:
#
# This is a version of state_networks_projections_1G_10G.ipynb that as flat pricing. That is, all districts, regardless if they are meeting or not meeting 1 Mbps goal will get a pricing of $890 for 1G or $2500 for 10G and how this compares to what they are already spending.
#
# ### Drive by request:
#
# See details for GA and NE. How were these estimates determined? Add upstream cost, and ia total costs for reference
#
# https://www.pivotaltracker.com/story/show/168150168

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
myConnection = psycopg2.connect( host=HOST_DAR,
                                user=USER_DAR,
                                password=PASSWORD_DAR,
                                database=DB_DAR,
                                port=PORT_DAR)

# sql_query_part1
sql_query = """
select
  d.district_id,
  d.funding_year,
  d.name,
  d.state_code,
  d.num_students,
  dbw.ia_bw_mbps_total,
  dbw.ia_monthly_cost_total,
  dbw.projected_bw_fy2018,
  dffa.fit_for_ia,
  dffa.fit_for_ia_cost,
  ss.sea_name,
  ss.name as ss_name,
  ss.state_network
from
  ps.districts d
  JOIN ps.districts_bw_cost dbw on d.funding_year = dbw.funding_year
  and d.district_id = dbw.district_id
  join ps.districts_fit_for_analysis dffa ON d.funding_year = dffa.funding_year
  and d.district_id = dffa.district_id
  join ps.states_static ss on d.state_code = ss.name
where
  d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
  and ss.state_network_natl_analysis = true
"""
# suppressing SettingWithCopyWarning
pd.options.mode.chained_assignment = None

#pull bandwidths from DB
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=names)

# convert from decimal to numeric
numeric_cols = ['num_students', 'ia_monthly_cost_total', 'projected_bw_fy2018']
df[numeric_cols] = df[numeric_cols].astype(float)


# ### Masks/Filters
mask_less_than_1k_students = df.num_students <= 1000
mask_1k_to_10k = (df.num_students > 1000) & (df.num_students <= 10000)
mask_10k_plus = df.num_students > 10000
mask_fit_ia = df.fit_for_ia == True
mask_fit_cost = df.fit_for_ia_cost == True


# ### Add Multiple of 10G's Needed to meet 1 Mbps Goal
# districts with 10k plus students
sub_cols = ['district_id', 'funding_year', 'name', 'state_code','num_students',
            'ia_bw_mbps_total', 'projected_bw_fy2018']
df_10k_plus = df[sub_cols][mask_10k_plus & mask_fit_ia & mask_fit_cost]

# change the price as needed
projected_price_1g = 890
projected_price_10g = 2500

# how many multiples of 10G to get them to their goals? with oversubscription
def round_up_nearest_10k(row):
    return int(math.ceil(row/10000))

# without oversubscription
df_10k_plus['multiple_10G_no_oversub'] = df_10k_plus.num_students.apply(round_up_nearest_10k)

# projected cost to get 10k plus to meet 1 Mbps without oversub by district
df_10k_plus['projected_mrc_10g_10kplus_no_oversub'] = df_10k_plus['multiple_10G_no_oversub']*projected_price_10g


# filtering for fit_for_ia and fit_for_ia_cost
df_fit = df[mask_fit_ia & mask_fit_cost]

# ### Add Projected Costs for 1G and 10G
def projected_cost_1G(row):
    if row <= 1000:
        return projected_price_1g

def projected_cost_10G(row):
    if (row > 1000) and (row <= 10000):
        return projected_price_10g

# new mrc with $890 1G projected price
df_fit['projected_mrc_1g'] = df_fit.num_students.apply(projected_cost_1G)

# new mrc with $2500 per 10G projected price
df_fit['projected_mrc_10g'] = df_fit.num_students.apply(projected_cost_10G)


# ### Add Projected Costs for 10G plus
# merge with df_ga_ne_cost
sub_cols = ['district_id', 'multiple_10G_no_oversub',  'projected_mrc_10g_10kplus_no_oversub']
df_result = pd.merge(df_fit, df_10k_plus[sub_cols], how='left', on=['district_id'])

# fill in nans with zeroes
df_result.fillna(0, inplace=True)


# ### Add Total Projected Cost
# total projected cost is the sum of 1g, 10g and 10gplus
df_result['total_projected_cost'] = df_result['projected_mrc_1g'] + df_result['projected_mrc_10g'] + df_result['projected_mrc_10g_10kplus_no_oversub']

# sort by state and district name
df_result.sort_values(['state_code', 'name'], inplace=True)

# save as csv
# os.chdir(GITHUB + '/Projects/sots-isl/data/')
# df_result.to_csv('id6010_state_network_cost_projection_no_isp_detailed_list.csv', index=False)


# ### Aggregating by State
# aggregate ia_monthly_cost_total and total_projected_cost by state
df_agg = df_result.groupby('state_code').agg({'ia_monthly_cost_total': 'sum', 'total_projected_cost': 'sum'})

# add already spending boolean column
df_agg['already_spending'] = df_agg['ia_monthly_cost_total'] >= df_agg['total_projected_cost']

# reset index
df_agg.reset_index(inplace=True)


# ### Adding Pretty Print

# format thousands as string
cols_to_pp = ['ia_monthly_cost_total', 'total_projected_cost']

def format_k_MM(row):
    if row >= 1000000:
        row_to_format = row/1000000
        return '{:,.1f}M'.format(row_to_format)
    else:
        row_to_format = row/1000
        return '{:,.0f}k'.format(row_to_format)

for col in cols_to_pp:
    df_agg[col+'_pp'] = df_agg[col].apply(format_k_MM)

# ### Plot: Current Total IA Spending vs. Total Projected Cost (No ISP Cost included)
# inputs
sort_col = 'total_projected_cost'
compare_col = 'ia_monthly_cost_total'
already_spending_col = 'already_spending'
df_input = df_agg.sort_values(['already_spending', sort_col])

# customize colors
colors = []
for proj_cost, orig_cost in zip(df_input[sort_col], df_input[compare_col]):
    if orig_cost >= proj_cost:
        colors.append('#c44f27')
    else:
        colors.append('#f9d2a3')

fig, ax = plt.subplots(figsize=(15, 17))

bar_size = 0.4

y_locs = np.arange(df_input.shape[0])

rects1 = ax.barh(y_locs, df_input[sort_col], align='edge', edgecolor='white', height=bar_size, color=colors, label="Projected Cost")
rects2 = ax.barh(y_locs - bar_size, df_input[compare_col], edgecolor='white', align='edge', height=bar_size, color='grey', label="Current IA Spending", alpha=0.5)

# pretty plot
plt.yticks(y_locs, df_input.state_code);
plt.yticks(y_locs, df_input.state_code);
for ticklabel, tickcolor in zip(plt.gca().get_yticklabels(), colors):
    ticklabel.set_color('grey')
    ticklabel.set_fontsize(12)
    if tickcolor == '#c44f27':
        ticklabel.set_color(tickcolor)
        ticklabel.set_fontweight('bold')

plt.title("Current State Network Spending (Total IA) vs. Projected Cost (ISP Cost NOT Included)", fontsize=14)
plt.tick_params(top=False, bottom=False, left=False, right=False, labelleft=True, labelbottom=False)
plt.box(None)
plt.legend(loc='upper right');

# add labels
for y0, y1, v0, v1, label_proj, label_sn in zip(y_locs, y_locs - bar_size,
                                            df_input[sort_col], df_input[compare_col],
                                                df_input[sort_col+'_pp'], df_input[compare_col+'_pp']):
    plt.text(v0, y0, label_proj, ha='left', va='bottom')
    plt.text(v1, y1, label_sn, ha='left', va='bottom')


# add conclusion
plt.text(0.6*max(df_input[compare_col]), 9,
         'Number of state networks already spending: ', fontsize=14,
         bbox=dict(facecolor='none', edgecolor='black', boxstyle='round,pad=2.5'));

plt.text(0.7*max(df_input[compare_col]), 8.5,
         str(df_input[already_spending_col].sum()) \
         + " (out of " + str(df_input.shape[0]) + ")", fontsize=14, fontweight='bold', color='#c44f27');

# save figure
os.chdir(GITHUB + '/Projects/sots-isl/figure_images/')
plt.savefig("id6009_state_network_cost_projection_no_isp_plot.png", bbox_inches='tight')
