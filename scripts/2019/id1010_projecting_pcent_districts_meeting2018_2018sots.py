from __future__ import division
import os
import psycopg2
import pandas as pd
import numpy as np
import warnings

from scipy.optimize import curve_fit

# connection credentials
HOST = os.environ.get("HOST_DAR_PROD")
USER = os.environ.get("USER_DAR_PROD")
PASSWORD = os.environ.get("PASSWORD_DAR_PROD")
DB = os.environ.get("DB_DAR_PROD")
GITHUB = os.environ.get("GITHUB")

# create cursor connection
myConnection = psycopg2.connect( host=HOST, user=USER, password=PASSWORD, database=DB)
cur = myConnection.cursor()

# functions to get specific tables
def get_districts():
    query = "select * from ps.districts"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_d = pd.DataFrame(rows, columns=names)
    return df_d

def get_districts_bw_cost():
    query = "select * from ps.districts_bw_cost"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_bw_cost = pd.DataFrame(rows, columns=names)
    return df_bw_cost

def get_districts_fit_for_analysis():
    query = "select * from ps.districts_fit_for_analysis"
    cur.execute(query)
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df_fit = pd.DataFrame(rows, columns=names)
    return df_fit


# get tables from dar prod database
df_d = get_districts()
df_fit = get_districts_fit_for_analysis()
df_bw_cost = get_districts_bw_cost()
cur. close()

 # Select subset of columns
df_d_cols = ['district_id', 'funding_year', 'district_type', 'in_universe']

df_fit_cols = ['district_id', 'funding_year', 'fit_for_ia', 'fit_for_ia_cost']

df_bw_cost_cols = ['district_id', 'funding_year','meeting_2018_goal_oversub']


# merge dataframes
merge_cols = ['district_id', 'funding_year']
df = df_d[df_d_cols].merge(df_fit[df_fit_cols],
                         on=merge_cols).merge(df_bw_cost[df_bw_cost_cols],
                         on=merge_cols)

# filter the database
df_filtered_ia = df[(df.in_universe==True) &
                  (df.district_type=='Traditional') &
                  (df.fit_for_ia==True)]

# Part 1: Get previous values of pcent districts meeting 2019 goal 2015-2019
# number of districts meeting 2019 goal by funding year
s1 = df_filtered_ia[df_filtered_ia.meeting_2018_goal_oversub == True].groupby('funding_year').district_id.nunique()

# number of total districts by funding year
s2 = df_filtered_ia.groupby('funding_year').district_id.nunique()

# concatenate series into a DataFrame
df_prev_results = pd.concat([s1, s2], axis=1).reset_index()
df_prev_results.columns = ['funding_year', 'num_districts_meeting2019', 'num_districts_total']

# calculating percents
df_prev_results.loc[:, 'pcent_meeting'] = df_prev_results['num_districts_meeting2019']/df_prev_results['num_districts_total']


# Part 2: Project districts meeting with action
with warnings.catch_warnings():
    warnings.filterwarnings("ignore")
    def func(x, a, b, c):
        return a*np.log(c+x)+b

    xdata = np.arange(1, 10)
    pcent_meeting_2014 = [30, ((77-30)*(2/3))+30,77, 88, 94, 97, 99, 99.8, 100]  #added extra values in end

    #fitting a curve
    popt, pcov = curve_fit(func, xdata, pcent_meeting_2014)

# calculating projection with action, capping at 100
x_new = np.arange(1, 16)
y_proj_action = []
for x in x_new:
    y = popt[0]*np.log(popt[2]+x)+popt[1]
    if y > 100:
        y = 100
        y_proj_action.append(y)
    else:
        y_proj_action.append(y)


# Part 3: Projecting values for 'No Action'
# 'No Action' assumes a linear model
# Linear
def func(x, a, b):
    return (a*x)+b

xdata = np.arange(0, 4)
pcent_meeting2019 = [9, 14, 22, 28]

#fitting a curve
popt, pcov = curve_fit(func, xdata, pcent_meeting2019)

# projecting for new values
x_new = np.arange(3, 18)

# calculating projection with action
y_proj_no_action = []
for x in x_new:
    y = popt[0]*x+popt[1]
    if y > 100:
        y = 100
        y_proj_no_action.append(y)
    else:
        y_proj_no_action.append(y)


# Summarizing results in df for future plotting
# getting pcent meeting from 2015 to 2019
all_pcents = list(df_prev_results.pcent_meeting.values*100)

# save results as a dataframe
df_results = pd.DataFrame({'funding_year': np.arange(2015, 2033),
                           'projected_pcent_meeting2019_no_action': all_pcents+y_proj_no_action[1:],
                           'projected_pcent_meeting2019_action': all_pcents+y_proj_action[1:]})

# # save csv
os.chdir(GITHUB + '/''data')
df_results.to_csv("id1010_projecting_pcent_districts_meeting2018_2018sots.csv", index=False)
