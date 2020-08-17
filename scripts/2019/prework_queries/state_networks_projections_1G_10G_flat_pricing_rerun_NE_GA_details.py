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


# ### Add Multiple of 10G's Needed to meet 1 Mbps Goal

# districts with 10k plus students
sub_cols = ['district_id', 'funding_year', 'name', 'state_code','num_students', 'ia_bw_mbps_total', 'projected_bw_fy2018']
df_10k_plus = df[sub_cols][mask_10k_plus]

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


# ### Filter by State
# filtering for GA and NE
df_ga_ne = df_10k_plus[df_10k_plus.state_code.isin(["GA", "NE"])]


# ### How much is the state network spending?

# ### For all state network states,
# - how much is the state network spending?
# - where possible, please calculate this at an application level rather than a district level. this should really just be the upstream line items serving districts in our universe, from the state network applicaiton.

# list of most common consortia ids

# 1006162

# 1004592
# 1008357
# 1009239
# 1014118
# 1015511
# 1020107
# 1016880
# 1020220
# 1021110
# 1032821
# 1037707
# 1035776
# 1055594
# 1051850 | 1051855
# 1049045
# 1047587
# 1047087


# ### Evan Follow up: Adding the upstream cost by district

# upstream monthly cost total by district

sql_query = """
select
  dli.district_id,
  d.funding_year,
  ss.state_code,
  sum(dli.total_monthly_cost) as upstream_monthly_cost_total

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
group by dli.district_id, d.funding_year, ss.state_code
"""

#pull bandwidths from DB
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df_sn_spending_district = pd.DataFrame(rows, columns=names)

# filtering for just states "GA" and "NE"
df_upstream_ga_ne = df_sn_spending_district[df_sn_spending_district.state_code.isin(['NE', 'GA'])]

# filtering original df for just states "GA" and "NE"
df_ga_ne = df[['district_id', 'funding_year', 'name', 'state_code',
               'num_students','ia_bw_mbps_total', 'ia_monthly_cost_total']][df.state_code.isin(['NE', 'GA'])]


# ### Rerun here if needed
# merge both dataframes
df_ga_ne_cost = pd.merge(df_ga_ne, df_upstream_ga_ne[['district_id', 'upstream_monthly_cost_total']],
                         on='district_id', how='left')


# ### Add Projected Costs for 1G and 10G
def projected_cost_1G(row):
    if row <= 1000:
        return projected_price_1g

def projected_cost_10G(row):
    if (row > 1000) and (row <= 10000):
        return projected_price_10g

# new mrc with $890 1G projected price
df_ga_ne_cost['projected_mrc_1g'] = df_ga_ne_cost.num_students.apply(projected_cost_1G)

# new mrc with $2500 per 10G projected price
df_ga_ne_cost['projected_mrc_10g'] = df_ga_ne_cost.num_students.apply(projected_cost_10G)


# ### Add Projected Costs for 10G plus
# merge with df_ga_ne_cost
sub_cols = ['district_id', 'multiple_10G_no_oversub',  'projected_mrc_10g_10kplus_no_oversub']
df_result_ga_ne = pd.merge(df_ga_ne_cost, df_10k_plus[sub_cols], how='left', on=['district_id'])

# fill in nans with zeroes
df_result_ga_ne.fillna(0, inplace=True)


# ### Add Total Projected Cost
# total projected cost is the sum of 1g, 10g and 10gplus
df_result_ga_ne['total_projected_cost'] = df_result_ga_ne['projected_mrc_1g'] + df_result_ga_ne['projected_mrc_10g'] + df_result_ga_ne['projected_mrc_10g_10kplus_no_oversub']

# save as a csv
os.chdir(GITHUB + '/''data')
df_result_ga_ne.to_csv("id6007_state_network_detailed_list_w_cost.csv", index=False)
