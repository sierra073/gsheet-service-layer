


##import modules
import os
import psycopg2 as psy
import matplotlib.pyplot as plt
import numpy as np 
import pandas as pd


# In[11]:


##set working directory
ficher = os.environ.get("FICHER")
print(os.environ.get("FICHER"))
os.chdir(ficher+"/''scripts/2019")

## db credentials
HOST_PROD = os.environ.get("HOST_DAR")
USER_PROD = os.environ.get("USER_DAR")
PASSWORD_PROD = os.environ.get("PASSWORD_DAR")
DB_PROD = os.environ.get("DB_DAR")
print(ficher)
print(HOST_PROD)


# In[12]:


##Query db for current frn info
myConnection = psy.connect(dbname=DB_PROD, user=USER_PROD, password=PASSWORD_PROD, host=HOST_PROD, port='5432')

cur = myConnection.cursor()

def query_script(file):
    sql = open(file)
    query = sql.read()
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=names)
    sql.close()
    return df
states_1mbps_df = query_script("id4002_state_level_1mbps.sql")


# In[13]:


current_states_df = states_1mbps_df[(states_1mbps_df['funding_year'] == 2019)]

##converting columns to numeric
numeric_cols = ['pct_districts_meeting_1mbps','pct_students_meeting_1mbps','median_ia_bandwidth_per_student_kbps','median_ia_monthly_cost_per_mbps','increase_pct_dist_meeting_1mbps','max_year_pct_increase','pct_rural','pct_small']

current_states_df[numeric_cols] = current_states_df[numeric_cols].apply(pd.to_numeric, errors='coerce')


# In[14]:


##adding column for state meeting goals categories
def goal_cat(row):
    if row['pct_districts_meeting_1mbps'] >= .5:
        return 'Majority Meeting'
    elif row['pct_districts_meeting_1mbps'] < .5 and row['pct_districts_meeting_1mbps'] >= .2:
        return 'Normal'
    elif row['pct_districts_meeting_1mbps'] < .2:
        return 'Lagging'
    else: 
        return None
current_states_df['meeting_category'] = current_states_df[['pct_districts_meeting_1mbps']].apply(goal_cat, axis=1)

## adding column for growth category 
def growth_cat(row):
    if row['max_year_pct_increase'] >= .2:
        return 'High Growth'
    elif row['max_year_pct_increase'] >= .1:
        return 'Normal'
    elif row['max_year_pct_increase'] < .1:
        return 'Lagging'
    else:
        return None
current_states_df['growth_category'] = current_states_df[['max_year_pct_increase']].apply(growth_cat, axis=1)


##define model state if >50% of districts are meeting or they have had a high growth year
def model_bool(row):
    if (row['meeting_category'] == 'Majority Meeting') | (row['growth_category'] == 'High Growth'):
        return True
    else:
        return False
    
current_states_df['model_state'] = current_states_df[['meeting_category','growth_category']].apply(model_bool, axis=1)

##define states that have not made progress towards 1 mbps goal
def lag_bool(row):
    if (row['meeting_category'] == 'Lagging' and row['growth_category'] == 'Lagging'):
        return True
    else:
        return False
    
current_states_df['lagging_state'] = current_states_df[['meeting_category','growth_category']].apply(lag_bool, axis=1)

##rounding long decimals 
decimals = pd.Series( ([3]*len(numeric_cols)) , index=numeric_cols)
current_states_df = current_states_df.round(decimals)


# In[15]:


model_states_df = current_states_df[(current_states_df['model_state'] == True)]
model_states_df.head()

lagging_states_df = current_states_df[(current_states_df['lagging_state'] == True)]
lagging_states_df.head()

example_states_df = current_states_df[(current_states_df['model_state'] == True) |(current_states_df['lagging_state'] == True)]
example_states_df


# In[16]:


##code for reviewing states_df (no actual output)
sorted_states_df = current_states_df.sort_values(by='max_year_pct_increase',ascending=False)


print(sorted_states_df[['state_code','pct_districts_meeting_1mbps','max_year_pct_increase','pct_rural','pct_small']])
##extra cols
#,'pct_students_meeting_1mbps','median_ia_bandwidth_per_student_kbps','median_ia_monthly_cost_per_mbps'


# In[17]:


output_df = current_states_df[['state_code','procurement_pattern','meeting_category','growth_category','model_state','lagging_state']]
output_df.to_csv('../../data/id4003_states_categories.csv', index=False)

