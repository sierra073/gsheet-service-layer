#!/usr/bin/env python
# coding: utf-8

# ### SAT-2906 - Historical distribution of BW increase
# 
# follow up from slides showing Tipping point analysis in Fiber Funnel section.
# 
# - When people upgrade, what’s the distribution of their BW increase? History of when people upgrade at all, not just to 1 Mbps. 
# - If we compare the distributions of people upgrading to those that need to upgrade and they are less overall, that would be compelling.
# 
# Needed for insights 3 Tuesday.

# In[1]:


from __future__ import division
from collections import defaultdict

import numpy as np
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
PORT_DAR = os.environ.get("PORT_DAR")
GITHUB = os.environ.get("GITHUB")


# In[3]:


#query data
def getData(conn, filename):

    cur = conn.cursor()
    cur.execute(open(filename, "r").read())
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=names)

#open connection to DB
myConnection = psycopg2.connect( host=HOST_DAR, 
                                user=USER_DAR, 
                                password=PASSWORD_DAR, 
                                database=DB_DAR, 
                                port=PORT_DAR)



#pull bandwidths from DB
df = getData(myConnection, 'bw_increase.sql')


# In[4]:


df.head()


# ### Convert Decimals to Floats

# In[5]:


numeric_cols = ['num_students', 'ia_bw_mbps_total']
df[numeric_cols] = df[numeric_cols].astype(float)


# ### Masks/Filters

# In[6]:


# usual filters
mask_traditional = df.district_type == 'Traditional'
mask_fit_ia = df.fit_for_ia == True
mask_fit_cost = df.fit_for_ia_cost == True
mask_in_universe = df.in_universe == True

# meeting goal filters
mask_not_meeting = df.meeting_2018_goal_oversub == False
mask_meeting = df.meeting_2018_goal_oversub == True

# upgrade indicators
upgrade_2016 = (df.funding_year == 2016) & (df.upgrade_indicator == True)
upgrade_2017 = (df.funding_year == 2017) & (df.upgrade_indicator == True)
upgrade_2018 = (df.funding_year == 2018) & (df.upgrade_indicator == True)
upgrade_2019 = (df.funding_year == 2019) & (df.upgrade_indicator == True)


# ### Plotting the Bandwidth Increases

# In[7]:


teals = ['#bfe6ef', '#6acce0', '#009296', '#004f51']
oranges = ['#fac4a5', '#f26c23', '#c44f27', '#83351a']


# ### All Districts, Percent Axis 

# In[8]:


# create resultant dictionary
df_result_all = defaultdict()

for fy_prev, upgrade_year in zip([2015, 2016, 2017, 2018], [upgrade_2016, upgrade_2017, upgrade_2018, upgrade_2019]):
    # upgrade indicator true for specific year
    # get district_ids
    df_temp = df[mask_traditional & 
                 mask_fit_ia & 
                 mask_in_universe & 
                 upgrade_year ]
    d_id_upgraders = df_temp.district_id.unique()

    # get the previous year and funding_year and `ia_bw_mbps_total`
    df_temp_prev = df[mask_traditional & mask_fit_ia & mask_in_universe & 
       (df.funding_year == fy_prev) & 
       df.district_id.isin(d_id_upgraders)][['district_id', 'ia_bw_mbps_total']]

    # rename `ia_bw_mbps_total_previous`
    df_temp_prev.columns = ['district_id', 'ia_bw_mbps_total_previous']

    # set index to district_id
    df_temp.set_index('district_id', inplace=True)
    df_temp_prev.set_index('district_id', inplace=True)

    # concat this as a new column to the original dataframe
    df_result_all[str(fy_prev+1)] = pd.concat([df_temp, df_temp_prev], axis=1, join='inner')
    
    # take the difference of `ia_bw_mbps_total` in 2019 and the year before 2018 
    # and call this new column `bw_increase`
    df_result_all[str(fy_prev+1)].loc[:, 'bw_increase'] = df_result_all[str(fy_prev+1)]['ia_bw_mbps_total'] - df_result_all[str(fy_prev+1)]['ia_bw_mbps_total_previous']

    # reset index
    df_result_all[str(fy_prev+1)].reset_index(inplace=True)


# In[9]:


# combine all dataframes into one 
df_result_all_combined = pd.concat([df_result_all['2016'], df_result_all['2017'], df_result_all['2018'], df_result_all['2019']])

# add 'bw_pct_change'
df_result_all_combined.loc[:, 'bw_pct_change'] = ((df_result_all_combined.loc[:, 'ia_bw_mbps_total'] - df_result_all_combined.loc[:, 'ia_bw_mbps_total_previous'])/df_result_all_combined.loc[:, 'ia_bw_mbps_total_previous'])*100


# ### Plotting the distribution

# In[30]:


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
df_result_all_combined['bw_pct_change_category'] = pd.cut(df_result_all_combined['bw_pct_change'], 
                                                     bins=bins, 
                                                     labels=labels[:-1], right=False)

# add new category
df_result_all_combined['bw_pct_change_category'] = df_result_all_combined['bw_pct_change_category'].cat.add_categories(str(max_bin-increment) + '% or over')

# fill in Nans (anything over max_bin)
df_result_all_combined[['bw_pct_change_category']] = df_result_all_combined[['bw_pct_change_category']].fillna(value=str(max_bin-increment) +'% or over')


# change index to string, and count values for plotting
df_result_all_combined['bw_pct_change_category'] = df_result_all_combined['bw_pct_change_category'].astype(str)['bw_pct_change_category'] = df_result_all_combined['bw_pct_change_category'].astype(str)


# In[31]:


# converting list to df for merging
df_temp = pd.DataFrame({'labels': labels})
df_temp.set_index('labels', inplace=True)

# count instances of category for district count
df_pct_change = df_result_all_combined.bw_pct_change_category.value_counts().to_frame()


# In[32]:


# merge into resultant dataframe
df_pct_change = df_temp.merge(df_pct_change, left_index=True, right_index=True )
df_pct_change.reset_index(inplace=True)

# rename index column
df_pct_change.columns = ['pct_category', 'district_count']

# add percentage column
df_pct_change['pct_by_category'] = df_pct_change['district_count']/df_pct_change['district_count'].sum()


# ### Plotting by Bw Increase Percentage

# In[33]:


df_pct_change


# In[52]:


# plotting the figure
plt.figure(figsize=(17, 7))
xmarks = np.arange(0, df_pct_change.shape[0])
plt.bar(xmarks, df_pct_change['district_count'], color='#009296')

# ticks, titles, axis labels
plt.ylabel("Percent/Number of Districts")
plt.xticks(xmarks, df_pct_change.pct_category.values, rotation=20)
plt.box(on=None)
plt.yticks([])

# add sum labels
for x0, v0, label, pct_label in zip(xmarks, 
                                    df_pct_change['district_count'], 
                                    df_pct_change['district_count'],
                                    df_pct_change['pct_by_category']
                                   ):
    plt.text(x0, v0, round(label), ha='center', va='bottom', color='orange')
    plt.text(x0, v0+ 100, str(round(pct_label*100, 1))+'%', ha='center', va='bottom')
    
plt.text(15, 3500, "Percent Bandwidth Increase ", ha='center', va='bottom', fontsize=12)
plt.text(16, 3200, "Median: "+str(df_result_all_combined.bw_pct_change.median()), ha='center', va='bottom')
plt.text(16, 3000, "Mean: "+str(round(df_result_all_combined.bw_pct_change.mean(), 1)), ha='center', va='bottom');
    
# save
plt.savefig("pct_bw_increase_whiteboard_fup_072319.png", bbox_inches = 'tight')


# In[ ]:




