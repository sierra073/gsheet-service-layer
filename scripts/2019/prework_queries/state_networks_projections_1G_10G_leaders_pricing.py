#!/usr/bin/env python
# coding: utf-8

# ### State networks already spending enough using leaders pricing

# Why:
# - Evan's story for state networks is that they just need to ensure their upstream circuits are either 1G or 10G, and prices are better than ever!
# 
# What:
# - Evan wants to know if we can make the argument that is what state leaders are paying, and can we show that state networks states are already spending that?
# 
# - he originally asked, where the various state networks would be on % of districts meeting 1 mbps if they all had the price of the leaders? but i followed up with this offering, which i think he will be happy with.
# 
# How:
# - first review surafaels leader $ figures: https://docs.google.com/document/d/1lHY-sejLfTA8eDQIdh8SAvw-Tn96FeRhOqZhZKRwCok/edit
# 
# - first calculate, for all state network states, how much would it cost to get the state to get 1Mbps/student for all their districts at these prices?
# 
# - then calculate, for all districts in state network states, how much is being spent on IA for all districts in the state? this may not be 100% clean, so extrapolation will be necessary.
# 
# - then calculate, how many states are already spending enough according to these benchmarks?

# In[1]:


import math
import numpy as np
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt


# In[2]:


# get states that are in state network
HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
PORT_DAR = os.environ.get("PORT_DAR")
GITHUB = os.environ.get("GITHUB")


# In[3]:


#open connection to DB
myConnection = psycopg2.connect( host=HOST_DAR, 
                                user=USER_DAR, 
                                password=PASSWORD_DAR, 
                                database=DB_DAR, 
                                port=PORT_DAR)


# ### QA Task 1

# In[4]:


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


# ### Convert decimal to numeric

# In[5]:


# convert from decimal to numeric
numeric_cols = ['ia_monthly_cost_total', 'projected_bw_fy2018']
df[numeric_cols] = df[numeric_cols].astype(float)


# ### 1Mbps Leaders: $1.31

# ### Masks/Filters

# In[6]:


mask_fit = df.fit_for_ia == True
mask_fit_cost = df.fit_for_ia_cost == True


# In[7]:


# define aggregations
agg_dict = {'district_id': 'count', 'num_students': 'sum'}

# groupby state and aggregate
df_all = df.groupby('state_code').agg(agg_dict)
df_clean = df[mask_fit & mask_fit_cost].groupby('state_code').agg(agg_dict)
df_clean.columns = ['district_count_clean', 'num_students_clean']


# ### Cost extrapolation

# In[8]:


# population
num_population_cost = df.groupby('state_code')['ia_monthly_cost_total'].sum()
num_population_cost


# In[9]:


# sample
num_sample_cost = df[mask_fit & mask_fit_cost].groupby('state_code')['ia_monthly_cost_total'].agg([('ia_monthly_cost_total_clean','sum')])
num_sample_cost


# ### Projected bw to Meet 1 Mbps oversub

# In[10]:


total_projected_bw_fy2018 = df[['state_code','projected_bw_fy2018']].groupby('state_code')['projected_bw_fy2018'].agg([('total_projected_bw_fy2018', 'sum')])

total_projected_bw_fy2018_clean = df[mask_fit & mask_fit_cost][['state_code','projected_bw_fy2018']].groupby('state_code')['projected_bw_fy2018'].agg([('total_projected_bw_fy2018_clean', 'sum')])


# ### IA Costs

# In[11]:


# district_count_clean, num_students_clean, ia_monthly_cost_total_clean: fit_for_ia and fit_for_ia_cost are True
# merging dfs
df_results = pd.concat([df_all, df_clean, 
                        total_projected_bw_fy2018, total_projected_bw_fy2018_clean,
                        num_population_cost, num_sample_cost], axis=1)
df_results


# ### QA Task 2

# ### Projected costs

# In[12]:


# leader_price (subject to change)
leader_price = 1.31

# projected cost, clean, oversub
df_results['cost_leader_pricing_oversub_clean'] = df_results['total_projected_bw_fy2018_clean']*leader_price

# projected cost, clean, no oversub
df_results['cost_leader_pricing_no_oversub_clean'] = df_results['num_students_clean']*leader_price

# projected cost, not clean, oversub
df_results['cost_leader_pricing_oversub'] = df_results['total_projected_bw_fy2018']*leader_price

# projected cost, not clean, no oversub
df_results['cost_leader_pricing_no_oversub'] = df_results['num_students']*leader_price


# In[13]:


df_results


# ### Add Pretty Print for plotting

# In[14]:


# format thousands as string
cols_to_pp = ['ia_monthly_cost_total',
       'ia_monthly_cost_total_clean', 'cost_leader_pricing_oversub_clean',
       'cost_leader_pricing_no_oversub_clean', 'cost_leader_pricing_oversub',
       'cost_leader_pricing_no_oversub']

def format_k_MM(row):
    if row >= 1000000:
        row_to_format = row/1000000
        return '{:,.1f}M'.format(row_to_format)
    else:
        row_to_format = row/1000
        return '{:,.0f}k'.format(row_to_format)

for col in cols_to_pp:
    df_results[col+'_pp'] = df_results[col].apply(format_k_MM)


# In[15]:


df_results


# ### QA Task 3

# ### Adding already spending column

# In[16]:


# clean, oversub
df_results['already_spending_clean_oversub'] = df_results['ia_monthly_cost_total_clean'] >= df_results['cost_leader_pricing_oversub_clean']

# clean, no oversub
df_results['already_spending_clean_no_oversub'] = df_results['ia_monthly_cost_total_clean'] >= df_results['cost_leader_pricing_no_oversub_clean']

# not clean, oversub
df_results['already_spending_oversub'] = df_results['ia_monthly_cost_total'] >= df_results['cost_leader_pricing_oversub']

# not clean, no oversub
df_results['already_spending_no_oversub'] = df_results['ia_monthly_cost_total'] >= df_results['cost_leader_pricing_no_oversub']


# In[17]:


df_results


# ### QA Task 4

# ### Plots

# ### Plot 1: Sorted, Clean, oversub

# In[34]:


sort_col = 'cost_leader_pricing_oversub_clean'
compare_col = 'ia_monthly_cost_total_clean'
already_spending_col = 'already_spending_clean_oversub'
df_input = df_results.sort_values(sort_col)

# customize colors
colors = []
for proj_cost, orig_cost in zip(df_input[sort_col], df_input[compare_col]): # keys are the names of the boys
    if orig_cost >= proj_cost:
        colors.append('#c44f27')
    else:
        colors.append('#f9d2a3')

fig, ax = plt.subplots(figsize=(15, 15))

bar_size = 0.4

y_locs = np.arange(df_results.shape[0])

rects1 = ax.barh(y_locs, df_input[sort_col], align='edge', edgecolor='white', height=bar_size, color=colors, label="Projected Cost with Leaders Pricing")
rects2 = ax.barh(y_locs - bar_size, df_input.ia_monthly_cost_total_clean, edgecolor='white', align='edge', height=bar_size, color='grey', label="Current IA Spending", alpha=0.5)

# pretty plot
plt.yticks(y_locs, df_input.index);
for ticklabel, tickcolor in zip(plt.gca().get_yticklabels(), colors):
    ticklabel.set_color('grey')
    ticklabel.set_fontsize(12) 
    if tickcolor == '#c44f27':
        ticklabel.set_color(tickcolor)
        ticklabel.set_fontweight('bold')

plt.title("Current State Network Spending vs. Projected Cost With Leaders Pricing (Clean, Oversub)")
plt.tick_params(top=False, bottom=False, left=False, right=False, labelleft=True, labelbottom=False)
plt.box(None)
plt.legend(loc='center right');

# add labels
for y0, y1, v0, v1, label_proj, label_sn in zip(y_locs, y_locs - bar_size, 
                                            df_input[sort_col], df_input[compare_col],  
                                                df_input[sort_col+'_pp'], df_input[compare_col+'_pp']):
    plt.text(v0, y0, label_proj, ha='left', va='bottom')
    plt.text(v1, y1, label_sn, ha='left', va='bottom')
    

# add conclusion
plt.text(0.65*max(df_input[sort_col]), 5, 
         'Number of state networks already spending: ', fontsize=14);

plt.text(0.80*max(df_input[sort_col]), 4.3, 
         str(df_input[already_spending_col].sum()) \
         + " (out of " + str(df_input.shape[0]) + ")", fontsize=14, color='#c44f27', fontweight='bold');


# ### Plot 2: Sorted, Clean, No Oversub

# In[143]:


# inputs
sort_col = 'cost_leader_pricing_no_oversub_clean'
compare_col = 'ia_monthly_cost_total_clean'
already_spending_col = 'already_spending_clean_no_oversub'
df_input = df_results.sort_values(sort_col)

# customize colors
colors = []
for proj_cost, orig_cost in zip(df_input[sort_col], df_input[compare_col]): # keys are the names of the boys
    if orig_cost >= proj_cost:
        colors.append('#c44f27')
    else:
        colors.append('#f9d2a3')

fig, ax = plt.subplots(figsize=(15, 15))

bar_size = 0.4

y_locs = np.arange(df_results.shape[0])

rects1 = ax.barh(y_locs, df_input[sort_col], align='edge', edgecolor='white', height=bar_size, color=colors, label="Projected Cost with Leaders Pricing")
rects2 = ax.barh(y_locs - bar_size, df_input[compare_col], edgecolor='white', align='edge', height=bar_size, color='grey', label="Current IA Spending", alpha=0.5)

# pretty plot
plt.yticks(y_locs, df_input.index);
plt.title("Current State Network Spending vs. Projected Cost With Leaders Pricing (Clean, No Oversub)")
plt.tick_params(top=False, bottom=False, left=False, right=False, labelleft=True, labelbottom=False)
plt.box(None)
plt.legend(loc='center right');

# add labels
for y0, y1, v0, v1, label_proj, label_sn in zip(y_locs, y_locs - bar_size, 
                                            df_input[sort_col], df_input[compare_col],  
                                                df_input[sort_col+'_pp'], df_input[compare_col+'_pp']):
    plt.text(v0, y0, label_proj, ha='left', va='bottom')
    plt.text(v1, y1, label_sn, ha='left', va='bottom')
    

# add conclusion
plt.text(0.65*max(df_input[sort_col]), 5, 
         'Number of state networks already spending: ', fontsize=14);

plt.text(0.75*max(df_input[sort_col]), 4, 
         str(df_input[already_spending_col].sum()) \
         + " (out of " + str(df_input.shape[0]) + ")", fontsize=14);


# ### Plot 3: Sorted, Not Clean, Oversub

# In[142]:


sort_col = 'cost_leader_pricing_oversub'
compare_col = 'ia_monthly_cost_total'
df_input = df_results.sort_values(sort_col)

# customize colors
colors = []
for proj_cost, orig_cost in zip(df_input[sort_col], df_input[compare_col]): # keys are the names of the boys
    if orig_cost >= proj_cost:
        colors.append('#c44f27')
    else:
        colors.append('#f9d2a3')

fig, ax = plt.subplots(figsize=(15, 15))

bar_size = 0.4

y_locs = np.arange(df_results.shape[0])

rects1 = ax.barh(y_locs, df_input[sort_col], align='edge', edgecolor='white', height=bar_size, color=colors, label="Projected Cost with Leaders Pricing")
rects2 = ax.barh(y_locs - bar_size, df_input[compare_col], edgecolor='white', align='edge', height=bar_size, color='grey', label="Current IA Spending", alpha=0.5)

# pretty plot
plt.yticks(y_locs, df_input.index);
plt.title("Current State Network Spending vs. Projected Cost With Leaders Pricing (Not Clean, Oversub)")
plt.tick_params(top=False, bottom=False, left=False, right=False, labelleft=True, labelbottom=False)
plt.box(None)
plt.legend(loc='center right');

# add labels
for y0, y1, v0, v1, label_proj, label_sn in zip(y_locs, y_locs - bar_size, 
                                            df_input[sort_col], df_input[compare_col],  
                                                df_input[sort_col+'_pp'], df_input[compare_col+'_pp']):
    plt.text(v0, y0, label_proj, ha='left', va='bottom')
    plt.text(v1, y1, label_sn, ha='left', va='bottom')
    

# add conclusion
plt.text(0.65*max(df_input[sort_col]), 5, 
         'Number of state networks already spending: ', fontsize=14);

plt.text(0.75*max(df_input[sort_col]), 4, 
         str(df_input['already_spending_oversub'].sum()) \
         + " (out of " + str(df_input.shape[0]) + ")", fontsize=14);


# ### Plot 4: Sorted, Not Clean, No Oversub

# In[39]:


# inputs
sort_col = 'cost_leader_pricing_no_oversub'
compare_col = 'ia_monthly_cost_total'
already_spending_col = 'already_spending_no_oversub'
df_input = df_results.sort_values(sort_col)

# customize colors
colors = []
for proj_cost, orig_cost in zip(df_input[sort_col], df_input[compare_col]): # keys are the names of the boys
    if orig_cost >= proj_cost:
        colors.append('#c44f27')
    else:
        colors.append('#f9d2a3')

fig, ax = plt.subplots(figsize=(15, 15))

bar_size = 0.4

y_locs = np.arange(df_results.shape[0])

rects1 = ax.barh(y_locs, df_input[sort_col], align='edge', edgecolor='white', height=bar_size, color=colors, label="Projected Cost with Leaders Pricing")
rects2 = ax.barh(y_locs - bar_size, df_input[compare_col], edgecolor='white', align='edge', height=bar_size, color='grey', label="Current IA Spending", alpha=0.5)

# pretty plot
plt.yticks(y_locs, df_input.index);
plt.yticks(y_locs, df_input.index);
for ticklabel, tickcolor in zip(plt.gca().get_yticklabels(), colors):
    ticklabel.set_color('grey')
    ticklabel.set_fontsize(12) 
    if tickcolor == '#c44f27':
        ticklabel.set_color(tickcolor)
        ticklabel.set_fontweight('bold')

plt.title("Current State Network Spending vs. Projected Cost With Leaders Pricing (No Oversub)", fontsize=14)
plt.tick_params(top=False, bottom=False, left=False, right=False, labelleft=True, labelbottom=False)
plt.box(None)
plt.legend(loc='center right');

# add labels
for y0, y1, v0, v1, label_proj, label_sn in zip(y_locs, y_locs - bar_size, 
                                            df_input[sort_col], df_input[compare_col],  
                                                df_input[sort_col+'_pp'], df_input[compare_col+'_pp']):
    plt.text(v0, y0, label_proj, ha='left', va='bottom')
    plt.text(v1, y1, label_sn, ha='left', va='bottom')
    

# add conclusion
plt.text(0.65*max(df_input[sort_col]), 5, 
         'Number of state networks already spending: ', fontsize=14);

plt.text(0.8*max(df_input[sort_col]), 4.3, 
         str(df_input[already_spending_col].sum()) \
         + " (out of " + str(df_input.shape[0]) + ")", fontsize=14, fontweight='bold', color='#c44f27');


# In[ ]:


# 

