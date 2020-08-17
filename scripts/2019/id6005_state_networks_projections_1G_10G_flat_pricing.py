#!/usr/bin/env python
# coding: utf-8

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

import math
import numpy as np
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt


# For all state network states,
# - how much would it cost to get the state 1G for all of their districts with <1,000 students, 10G for 1,000-10,000 students?
# - for districts over 10,000 students, however many multiples of 10G get them to their goals? (evan has not requested adjustment for oversubscription, but an alternative we can try if these numbers are low are oversubscription adjustments).
#
# note that these projections may change, so it would be wise to make the cost for 1G and 10G easy to malipulate?

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


# ### QA Task 1

# For all state network states,
#
# - how much would it cost to get the state 1G for all of their districts with <1,000 students,
# - 10G for 1,000-10,000 students?

# sql_query_part1
sql_query = """
select
  d.district_id,
  d.funding_year,
  d.name,
  d.state_code,
  d.consortium_affiliation,
  d.consortium_affiliation_ids,
  d.num_students,
  dbw.ia_bw_mbps_total,
  dbw.ia_monthly_cost_total,
  dbw.ia_monthly_cost_per_mbps,
  dbw.projected_bw_fy2018,
  dbw.meeting_2018_goal_oversub,
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
  and ss.state_network = true
"""

#pull bandwidths from DB
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=names)

# convert from decimal to numeric
numeric_cols = ['num_students', 'ia_monthly_cost_total', 'ia_monthly_cost_per_mbps','projected_bw_fy2018']
df[numeric_cols] = df[numeric_cols].astype(float)

# ### Removing specific states with $0k state network spending: NC, AL, MS, CT, SC, WV, HI
df = df[~df.state_code.isin(['NC', 'AL', 'MS', 'CT', 'SC', 'WV', 'HI'])]

# ### Masks/Filters


mask_less_than_1k_students = df.num_students <= 1000
mask_1k_to_10k = (df.num_students > 1000) & (df.num_students <= 10000)
mask_10k_plus = df.num_students > 10000
mask_not_meeting = df.meeting_2018_goal_oversub == False
mask_meeting = df.meeting_2018_goal_oversub == True
mask_fit_ia = df.fit_for_ia == True
mask_fit_cost = df.fit_for_ia_cost == True


# ### States that can use an upgrade to 1G, Cost, and Savings

# number of districts  in each state
s_num_districts = df.groupby('state_code')['district_id'].count()

# number of districts with less than 1000 students in each state
s_1000_or_less = df[mask_less_than_1k_students].groupby('state_code')['district_id'].count()

# number of districts with 1000 to <10,000 students in each state
s_1k_to_10k = df[mask_1k_to_10k].groupby('state_code')['district_id'].count()

# number of districts with >10,000 students in each state
s_10k_plus = df[mask_10k_plus].groupby('state_code')['district_id'].count()

# concatenate all series into one df
df_results = pd.concat([s_num_districts,
                        s_1000_or_less,
                        s_1k_to_10k,
                        s_10k_plus], axis=1)

# rename columns
df_results.columns = ['num_districts',
                      'num_districts_1k_or_less',
                      'num_districts_1k_to_10k',
                      'num_districts_10k_plus'
                     ]

# fill in nans with zeros
df_results.fillna(0, inplace=True)

# ### QA Task 2

# ### QA Task 3

# ### Projected MRC with assumed prices for 1G, 10G

# change the price as needed
projected_price_1g = 890
projected_price_10g = 2500

# new mrc with $890 1G projected price
df_results['projected_mrc_1g'] = df_results['num_districts_1k_or_less']*projected_price_1g

# new mrc with $2500 per 10G projected price
df_results['projected_mrc_10g'] = df_results['num_districts_1k_to_10k']*projected_price_10g

# ### Add Multiple of 10G's Needed to meet 1 Mbps Goal

# districts with 10k plus students
sub_cols = ['district_id', 'funding_year', 'name', 'state_code','num_students', 'ia_bw_mbps_total', 'projected_bw_fy2018']
df_10k_plus = df[sub_cols][mask_10k_plus]
df_10k_plus


# ### QA Task 4

# how many multiples of 10G to get them to their goals? with oversubscription
def round_up_nearest_10k(row):
    return int(math.ceil(row/10000))

# with oversubscription
df_10k_plus['multiple_10G_oversub'] = df_10k_plus.projected_bw_fy2018.apply(round_up_nearest_10k)

# without oversubscription
df_10k_plus['multiple_10G_no_oversub'] = df_10k_plus.num_students.apply(round_up_nearest_10k)

# projected cost to get 10k plus to meet 1 Mbps with oversub by district
df_10k_plus['projected_mrc_10g_10kplus_oversub'] = df_10k_plus['multiple_10G_oversub']*projected_price_10g

# projected cost to get 10k plus to meet 1 Mbps without oversub by district
df_10k_plus['projected_mrc_10g_10kplus_no_oversub'] = df_10k_plus['multiple_10G_no_oversub']*projected_price_10g


# total projected mrc by state for 10k plus districts
# total cost by state to upgrade districts over 10k to meeting 1 mbps with projected cost (oversub)
s_10kplus_oversub = df_10k_plus.groupby('state_code')['projected_mrc_10g_10kplus_oversub'].sum()

# total projected mrc by state for 10k plus districts
# total cost by state to upgrade districts over 10k to meeting 1 mbps with projected cost (no oversub)
s_10kplus_no_oversub = df_10k_plus.groupby('state_code')['projected_mrc_10g_10kplus_no_oversub'].sum()


# concatenate to the resultant dataframe
# concat into one resultant dataframe
df_results = pd.concat([df_results, s_10kplus_oversub, s_10kplus_no_oversub], axis=1)

# fill in nans with zeroes
df_results.fillna(0, inplace=True)

# add total mrc - oversub
df_results['projected_mrc_total_oversub'] = df_results['projected_mrc_1g'] + df_results['projected_mrc_10g'] + df_results['projected_mrc_10g_10kplus_oversub']

# add total mrc - no oversub
df_results['projected_mrc_total_no_oversub'] = df_results['projected_mrc_1g'] + df_results['projected_mrc_10g'] + df_results['projected_mrc_10g_10kplus_no_oversub']

# ### QA Task 5

# ### How much is the state network spending?

# ### For all state network states,
# - how much is the state network spending?
# - where possible, please calculate this at an application level rather than a district level. this should really just be the upstream line items serving districts in our universe, from the state network applicaiton.

sql_query = """
select
  ss.state_code,
  sum(dli.total_monthly_cost) as state_network_mrc

from
  ps.districts_line_items dli
  JOIN ps.districts d on d.district_id = dli.district_id
  and d.funding_year = dli.funding_year
  join ps.states_static ss on d.state_code = ss.state_code
  join ps.line_items li on dli.line_item_id = li.line_item_id
  join ps.entity_bens_lkp eb on li.applicant_ben = eb.ben
  and li.funding_year = eb.funding_year

where
  d.funding_year = 2019
  and d.district_type = 'Traditional'
  and d.in_universe = true
  and ss.state_network = true
  and dli.purpose in ('upstream')
  and eb.entity_id in (
    1006162, 1004592, 1008357, 1009239,
    1014118, 1015511, 1020107, 1020220,
    1021110, 1032821, 1037707, 1035776,
    1055594, 1051855, 1047087,
    1051850, 1049045, 1047587
  )
group by
  1
"""

#pull bandwidths from DB
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df_sn_spending = pd.DataFrame(rows, columns=names)

# set state_code as index
df_sn_spending.set_index('state_code', inplace=True)

# merge dataframes
df_final = pd.concat([df_results, df_sn_spending], axis=1)

# fill in nan's with zeros
df_final.fillna(0, inplace=True)


# ### Add pretty print for plotting
# format thousands as string
df_final['projected_mrc_total_oversub_pp'] = (df_final['projected_mrc_total_oversub']/1000).apply(lambda x: '{:,.0f}k'.format(x))
df_final['state_network_mrc_pp'] = (df_final['state_network_mrc']/1000).apply(lambda x: '{:,.0f}k'.format(x))
df_final['projected_mrc_total_no_oversub_pp'] = (df_final['projected_mrc_total_no_oversub']/1000).apply(lambda x: '{:,.0f}k'.format(x))

# ### QA Task 6

# ### Already spending?

# already spending column (oversub)
df_final['already_spending_oversub'] = df_final.state_network_mrc >= df_final.projected_mrc_total_oversub

# already spending column (no oversub)
df_final['already_spending_no_oversub'] = df_final.state_network_mrc >= df_final.projected_mrc_total_no_oversub

# ### Conclusion
#print(f"Number of states in state network already spending enough to get ALL districts to upgrade to 1G or 10G (oversub): {df_final.already_spending_oversub.sum()} out of {df_final.shape[0]}")
#print(f"Number of states in state network already spending enough to get ALL districts to upgrade to 1G or 10G (oversub): {df_final.already_spending_no_oversub.sum()} out of {df_final.shape[0]}")


# ### QA Task 7

# ### Plotting
# ### Version 3 plot: sorted, no oversub

# reset and rename column
df_final_v3 = df_final.reset_index()
df_final_v3.columns = ['state_code','num_districts', 'num_districts_1k_or_less', 'num_districts_1k_to_10k',
       'num_districts_10k_plus', 'projected_mrc_1g', 'projected_mrc_10g',
       'projected_mrc_10g_10kplus_oversub',
       'projected_mrc_10g_10kplus_no_oversub', 'projected_mrc_total_oversub',
       'projected_mrc_total_no_oversub', 'state_network_mrc',
       'projected_mrc_total_oversub_pp', 'state_network_mrc_pp',
       'projected_mrc_total_no_oversub_pp', 'already_spending_oversub',
       'already_spending_no_oversub']

# sort values
proj_cost_col = 'projected_mrc_total_no_oversub'
orig_cost_col = 'state_network_mrc'
df_final_v3 = df_final_v3.sort_values(['already_spending_no_oversub', proj_cost_col], ascending=False)
df_input = df_final_v3.copy()

# customize colors
colors_1g = []
colors_10g = []
colors_10g_plus = []
for orig_cost, proj_cost in zip(df_input[orig_cost_col], df_input[proj_cost_col]):
    if orig_cost >= proj_cost:
        colors_1g.append('#fac4a5')
        colors_10g.append('#f26c23')
        colors_10g_plus.append('#c44f27')
    else:
        colors_1g.append('#fbe9bc')
        colors_10g.append('#f9d2a3')
        colors_10g_plus.append('#f5bc74')

# indices where bar plot will go
xmarks = np.arange(0, len(df_final_v3.index))

fig = plt.figure(figsize=(17, 10))
ax = fig.add_subplot(111)
width = np.min(np.diff(xmarks))/3

ax.bar(xmarks+width/2., df_final_v3.projected_mrc_1g, width, color=colors_1g, edgecolor='white', label='Projected Cost 1g')
ax.bar(xmarks+width/2., df_final_v3.projected_mrc_10g, width, bottom=df_final_v3.projected_mrc_1g, color=colors_10g, edgecolor='white', label='Projected Cost 10G')
ax.bar(xmarks+width/2., df_final_v3.projected_mrc_10g_10kplus_no_oversub, width, bottom=df_final_v3.projected_mrc_1g+df_final_v3.projected_mrc_10g, color=colors_10g_plus, edgecolor='white', label='Projected Cost 10G (10k plus)')
ax.bar(xmarks-width/2., df_final_v3.state_network_mrc, width, color='grey', edgecolor='white', label='Current State Network Spending', alpha=.5)

plt.xticks(xmarks, df_final_v3.state_code)
plt.title("Current State Network Spending vs. Projected MRC to Upgrade All to 1G, 10G (no oversub)")
plt.tick_params(top=False, bottom=False, left=False, right=False, labelleft=False, labelbottom=True)
plt.ylabel("Dollars ($)")
plt.box(None)
plt.legend()

# add sum labels
for x0, x1, v0, v1, label_proj, label_sn in zip(xmarks+width/2, xmarks-width/2,
                                            df_final_v3['projected_mrc_total_no_oversub'], df_final_v3['state_network_mrc'],
                                            df_final_v3['projected_mrc_total_no_oversub_pp'], df_final_v3['state_network_mrc_pp']):
    plt.text(x0, v0, label_proj, ha='center', va='bottom')
    plt.text(x1, v1, label_sn, ha='center', va='bottom')


# conclusion texts
plt.text(1.5, .95*int((max(df_final_v3.state_network_mrc))), "Number of states  ", fontsize=12)
plt.text(1.5, .92*int((max(df_final_v3.state_network_mrc))), "that are already spending: ", fontsize=12, color='black',
        bbox=dict(facecolor='none', edgecolor='black', boxstyle='round,pad=2'))
plt.text(2, .88*int((max(df_final_v3.state_network_mrc))), str(df_final_v3.already_spending_no_oversub.sum()) + " out of " + str(df_final_v3.shape[0]), fontsize=14)

# save to isl folder figure_images
os.chdir(GITHUB + '/Projects/sots-isl/figure_images/')
plt.savefig("id6005_state_networks_projections_1G_10G_flat_pricing.png", bbox_inches = 'tight')
