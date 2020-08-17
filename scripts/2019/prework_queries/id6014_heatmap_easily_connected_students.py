#!/usr/bin/env python
# coding: utf-8

# Pivotal card: https://www.pivotaltracker.com/story/show/168254684
#
# Chart #: State leadership can make digital learning a reality for ## million students by 2022
# [Insert 2 national heat maps showing where states are on 1 Mbps (% of districts or students) in 2019 and in 2022 if all of the above upgrades happen]
# also csv files of the dataframes of percent of districts and students meeting 1 mbps and/or "easily" connected


import numpy as np
import os
import psycopg2
import pandas as pd

# import plotly.graph_objects as go
# import plotly
# import plotly.offline as offline


HOST_DAR = "charizard-psql1.cyttrh279zkr.us-east-1.rds.amazonaws.com"
USER_DAR = "eshadmin"
PASSWORD_DAR = "J8IkWgrwsxC&"
DB_DAR = "sots_snapshot_2019_08_19"
PORT_DAR = os.environ.get("PORT_DAR")
GITHUB = os.environ.get("GITHUB")

# HOST_DAR = os.environ.get("HOST_DAR")
# USER_DAR = os.environ.get("USER_DAR")
# PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
# DB_DAR = os.environ.get("DB_DAR")
# PORT_DAR = os.environ.get("PORT_DAR")
# GITHUB = os.environ.get("GITHUB")

#open connection to DB
myConnection = psycopg2.connect(host=HOST_DAR,
                                user=USER_DAR,
                                password=PASSWORD_DAR,
                                database=DB_DAR)

# sql_query_part1
sql_query = """
select
  d.funding_year,
  d.district_id,
  d.name,
  d.city,
  d.state_code,
  d.latitude,
  d.longitude,
  d.num_students,
  d.num_schools
from
  ps.districts_upgrades du
  join ps.districts d on du.district_id = d.district_id
  and du.funding_year = d.funding_year
where
  du.path_to_meet_2018_goal_group = 'No Cost Peer Deal'
  and d.in_universe = true
  and d.district_type = 'Traditional'
  and d.funding_year = 2019
"""

# suppressing SettingWithCopyWarning
pd.options.mode.chained_assignment = None

#pull bandwidths from DB
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df_dar_no_cost_pd = pd.DataFrame(rows, columns=names)


# ### Add Jamie's csv addition
#
# - the ones with the best deals
# - csv output of python code 7024
path = GITHUB + '''scripts/2019/prework_queries/'
os.chdir(path)
df_temp = pd.read_csv('id7024_best_deal_districts.csv')
df_jamie_csv = df_temp[df_temp.funding_year == 2019]

# only take best deal
df_jamie_csv = df_jamie_csv[df_jamie_csv.best_deal == True]

# add lats, longs, city from ps.districts
sql_query = """
select
  d.district_id,
  d.name,
  d.city,
  d.latitude,
  d.longitude,
  d.num_schools
from
  ps.districts d
where
  d.in_universe = true
  and d.district_type = 'Traditional'
  and d.funding_year = 2019
"""

#pull bandwidths from DB
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df_ps_districts = pd.DataFrame(rows, columns=names)

# merge
df_jamie_csv = df_jamie_csv.merge(df_ps_districts, on=['district_id'])

# reduce columns to only relevant ones
sub_cols = ['funding_year', 'district_id', 'name', 'city', 'state_code', 'latitude',
       'longitude', 'num_students', 'num_schools']
df_jamie_csv = df_jamie_csv[sub_cols]

# concatenating all of jamie's analysis
df_jamie = pd.concat([df_dar_no_cost_pd, df_jamie_csv], sort=True)


# #### Adding Surafel's analysis
df_temp = pd.read_excel(path+'Districts who can meet at future circuit prices.xlsx', sheet_name='Needs 1Gbps')
df_surafel_1g = df_temp[df_temp.status == 'Paying enough']
# merge with other columns
df_surafel_1g = df_surafel_1g[['state_code', 'district_id', 'num_students']].merge(df_ps_districts[['district_id', 'name', 'city', 'latitude', 'longitude', 'num_schools']], on='district_id')

df_temp = pd.read_excel(path+'Districts who can meet at future circuit prices.xlsx', sheet_name='Needs 10Gbps')
df_surafel_10g = df_temp[df_temp.status == 'Paying enough']
df_surafel_10g = df_surafel_10g[['state_code', 'district_id', 'num_students']].merge(df_ps_districts[['district_id', 'name', 'city', 'latitude', 'longitude', 'num_schools']], on='district_id')

df_temp = pd.read_excel(path+'Districts who can meet at future circuit prices.xlsx', sheet_name='Needs >10Gbps')
df_surafel_10g_plus = df_temp[df_temp.status == 'Paying enough']
df_surafel_10g_plus = df_surafel_10g_plus[['state_code', 'district_id', 'num_students']].merge(df_ps_districts[['district_id', 'name', 'city', 'latitude', 'longitude', 'num_schools']], on='district_id')

# concatenate all dataframes
frames = [df_jamie, df_surafel_1g, df_surafel_10g, df_surafel_10g_plus]
df_combo_js = pd.concat(frames, sort=True)

# sort by district_id
df_combo_js = df_combo_js.sort_values('district_id')

# fill in Nans in funding year with 2019
df_combo_js.funding_year.fillna(2019, inplace=True)

# remove duplicate rows
df_combo_js.drop_duplicates(inplace=True)


# ### Extrapolate Districts
sql_query = """
select
  d.funding_year,
  d.district_id,
  d.name,
  d.city,
  d.state_code,
  d.latitude,
  d.longitude,
  d.num_students,
  d.num_schools,
  d.in_universe,
  d.district_type,
  dffa.fit_for_ia,
  dffa.fit_for_ia_cost,
  dbw.meeting_2018_goal_no_oversub,
  dbw.meeting_2018_goal_oversub
from
  ps.districts d
  join ps.districts_fit_for_analysis dffa on d.district_id = dffa.district_id
  and d.funding_year = dffa.funding_year
  join ps.districts_bw_cost dbw on d.district_id = dbw.district_id
  and d.funding_year = dbw.funding_year
where
  d.funding_year = 2019
"""

#pull districts from DB
cur = myConnection.cursor()
cur.execute(sql_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df_d_dffa = pd.DataFrame(rows, columns=names)


# ### Districts Currently Meeting 1 Mbps 2019 - Detailed List
df_d_dffa['state_code'] = df_d_dffa['state_code'].astype('category')

# districts meeting 2019
df_2019_not_in_js = df_d_dffa[(df_d_dffa.fit_for_ia == True) &
                             (df_d_dffa.in_universe == True) &
                             (df_d_dffa['district_type']=='Traditional') &
                             (df_d_dffa.meeting_2018_goal_oversub == True) &
                             (df_d_dffa.state_code.isin(df_combo_js.state_code.unique())) &
                             (~df_d_dffa.state_code.isin(df_combo_js.district_id.unique()))]

# select subset of columns
df_2019_not_in_js = df_2019_not_in_js[list(df_combo_js.columns)]

# concatenate js and dar
frames = [df_2019_not_in_js, df_combo_js]
df_combo_dar_js = pd.concat(frames, sort=True)

# take population and sample
s_population_districts = df_d_dffa[(df_d_dffa.in_universe == True) &
                           (df_d_dffa['district_type']=='Traditional')].groupby('state_code').district_id.nunique()

s_sample_districts = df_d_dffa[(df_d_dffa.fit_for_ia == True) &
                         (df_d_dffa.in_universe == True) &
                         (df_d_dffa['district_type']=='Traditional')].groupby('state_code').district_id.nunique()


# groupby state for extrapolation
df_2023 = df_combo_dar_js.groupby('state_code').agg({'district_id': 'count', 'num_students': 'sum', })

# rename columns
df_2023.columns = ['num_districts_meeting', 'num_students_meeting']

# reset index
df_2023.reset_index(inplace=True)

# function to extrapolate dirty districts
def extrapolate_districts(row):
    return (row['num_districts_meeting']*s_population_districts[row['state_code']])/s_sample_districts[row['state_code']]

# total number of districts that can be easily connected
df_2023.loc[:, 'num_districts_newly_meeting_extrap'] = df_2023[['state_code',
                                                                'num_districts_meeting']].apply(extrapolate_districts, axis=1)
# set index as state code
df_2023.set_index('state_code', inplace=True)

# s_population_districts
df_2023 = pd.concat([df_2023, s_population_districts.reindex(df_2023.index)], axis=1)

# rename column
df_2023.rename(columns={'district_id': 'district_counts'}, inplace=True)

# add percentage columns
df_2023['pct_districts_meeting'] = (df_2023['num_districts_newly_meeting_extrap']/df_2023['district_counts'])*100


# ### Extrapolate Students
# take population and sample
s_population_students = df_d_dffa[(df_d_dffa.in_universe == True) &
                                  (df_d_dffa['district_type']=='Traditional')].groupby('state_code').num_students.sum()

s_sample_students = df_d_dffa[(df_d_dffa.fit_for_ia == True) &
                              (df_d_dffa.in_universe == True) &
                              (df_d_dffa['district_type']=='Traditional')].groupby('state_code').num_students.sum()


# reset index
df_2023.reset_index(inplace=True)

# function to extrapolate ia cost to dirty districts
def extrapolate_students(row):
    return (row['num_students_meeting']*s_population_students[row['state_code']])/s_sample_students[row['state_code']]

# total number of students that can be easily connected
df_2023.loc[:, 'num_students_newly_meeting_extrap'] = df_2023[['state_code',
                                                                            'num_students_meeting']].apply(extrapolate_students, axis=1)
# set index as state code
df_2023.set_index('state_code', inplace=True)

# s_population_districts
df_2023 = pd.concat([df_2023, s_population_students.reindex(df_2023.index)], axis=1)

# add percentage columns
df_2023['pct_students_meeting'] = (df_2023['num_students_newly_meeting_extrap']/df_2023['num_students'])*100

# reset index
df_2023.reset_index(inplace=True)

# add 2023 year
df_2023.loc[:, 'year'] = [2023]*df_2023.shape[0]


# ### Adding the 2019 districts that are meeting
df_2019 = df_d_dffa[(df_d_dffa.fit_for_ia == True) &
         (df_d_dffa.in_universe == True) &
         (df_d_dffa['district_type']=='Traditional') &
         (df_d_dffa.meeting_2018_goal_oversub == True)
         ].groupby('state_code').agg({'district_id': 'count',
                                      'num_students': 'sum'}).fillna(0).astype(int).reset_index()

df_2019 = df_2019[(df_2019.state_code != 'DC')]

# rename column
df_2019.rename(columns={'district_id': 'num_districts_meeting',
                        'num_students': 'num_students_meeting'}, inplace=True)

# set index to state_code
df_2019.set_index('state_code', inplace=True)

# s_population_districts
df_2019 = pd.concat([df_2019,
                     s_population_districts.reindex(df_2019.index),
                     s_population_students.reindex(df_2019.index)], axis=1)

# rename column
df_2019.rename(columns={'district_id': 'district_counts'}, inplace=True)

# add year 2019 column
df_2019.loc[:, 'year'] = [2019]*df_2019.shape[0]

# set index as state code
df_2019.reset_index(inplace=True)


# ### Extrapolate districts for 2019

# total number of districts that can be easily connected
df_2019.loc[:, 'num_districts_newly_meeting_extrap'] = df_2019[['state_code',
                                                                'num_districts_meeting']].apply(extrapolate_districts, axis=1)
# set index as state code
df_2019.set_index('state_code', inplace=True)

# add percentage columns
df_2019['pct_districts_meeting'] = (df_2019['num_districts_newly_meeting_extrap']/df_2019['district_counts'])*100

# set index as state code
df_2019.reset_index(inplace=True)


# ### Extrapolate students for 2019

# total number of students that can be easily connected
df_2019.loc[:, 'num_students_newly_meeting_extrap'] = df_2019[['state_code',
                                                               'num_students_meeting']].apply(extrapolate_students, axis=1)

# add percentage columns
df_2019['pct_students_meeting'] = (df_2019['num_students_newly_meeting_extrap']/df_2019['num_students'])*100

# concat with
df_districts_pct_js = pd.concat([df_2023, df_2019], sort=True)


# #### State Network Analysis

# updated 9/23/19
df_sn_agg = pd.read_csv(path+"summary_state_network_cost_092319.csv")

# add students meeting 2019
df_sn_agg['num_districts_meeting_2019'] = df_sn_agg.loc[:, 'district_counts'] - df_sn_agg.loc[:, 'num_districts_newly_meeting_extrap']

# 2019 students meeting
sub_cols = ['state_code', 'num_districts_meeting_2019']
df_2019 = df_sn_agg[sub_cols]

# add year column
df_2019['year'] = [2019]*df_2019.shape[0]

# left join to add district counts
df_2019 = df_2019.merge(df_sn_agg[['state_code','district_counts']], on='state_code')

# 2023 students meeting
sub_cols = ['state_code', 'district_counts']
df_2023 = df_sn_agg[sub_cols]
df_2023.columns = ['state_code', 'num_districts_meeting_2023']

# add year column
df_2023['year'] = [2023]*df_2023.shape[0]

# left join with district counts
df_2023 = df_2023.merge(df_sn_agg[['state_code','district_counts']], on='state_code')

# set state as index
df_2023.set_index('state_code', inplace=True)
df_2019.set_index('state_code', inplace=True)

# substitute 'num_students_meeting_2019' for CT and ME
df_2023.loc['CT', 'num_districts_meeting_2023'] = df_2019.loc['CT', 'num_districts_meeting_2019']
df_2023.loc['ME', 'num_districts_meeting_2023'] = df_2019.loc['ME', 'num_districts_meeting_2019']

# reset index and rename
df_2023.reset_index(inplace=True)
df_2023.rename(columns={"num_districts_meeting_2023": "num_districts_meeting"}, inplace=True)

df_2019.reset_index(inplace=True)
df_2019.rename(columns={"num_districts_meeting_2019": "num_districts_meeting"}, inplace=True)

# concat dfs
df_districts_pct = pd.concat([df_2019, df_2023]).reset_index(drop=True)

# divide for pct
df_districts_pct['pct_districts_meeting'] = (df_districts_pct['num_districts_meeting']/df_districts_pct['district_counts'])*100


# ### Concatenate dar, js, surafel, and kat into one

# concatenate only 2023
df_districts_pct_2023 = df_districts_pct[df_districts_pct.year == 2023]

# take subset columns
js_cols = ['state_code', 'num_districts_newly_meeting_extrap', 'year', 'district_counts', 'pct_districts_meeting']
df_districts_pct_js_subset = df_districts_pct_js[js_cols]

# rename columns
df_districts_pct_js_subset.rename(columns={"num_districts_newly_meeting_extrap": "num_districts_meeting"}, inplace=True)

# add hi 2023
df_temp_hi = df_districts_pct_js[df_districts_pct_js.state_code == 'HI']
df_temp_hi.replace({'year': {2019: 2023}}, inplace=True)
df_temp_hi_subset = df_temp_hi[js_cols]
df_temp_hi_subset.rename(columns={"num_districts_newly_meeting_extrap": "num_districts_meeting"}, inplace=True)

# add ms 2023
df_temp_ms = df_districts_pct_js[df_districts_pct_js.state_code == 'MS']
df_temp_ms.replace({'year': {2019: 2023}}, inplace=True)
df_temp_ms_subset = df_temp_ms[js_cols]
df_temp_ms_subset.rename(columns={"num_districts_newly_meeting_extrap": "num_districts_meeting"}, inplace=True)

# concatenate all
df_districts_pct_all = pd.concat([df_districts_pct_2023,
                                  df_districts_pct_js_subset,
                                  df_temp_hi_subset,
                                  df_temp_ms_subset
                                 ])

# round specific columns: num_districts_meeting and pct_districts_meeting
# rounded to nearest one
df_districts_pct_all['pct_districts_meeting_rounded'] = df_districts_pct_all['pct_districts_meeting'].round(0)
df_districts_pct_all['num_districts_meeting_rounded'] = df_districts_pct_all['num_districts_meeting'].round(0)


# ### Plotting: Heatmap
# rearrange columns and reset index
df_districts_pct_all = df_districts_pct_all[['state_code', 'year',
                                             'district_counts', 'num_districts_meeting',
                                             'pct_districts_meeting',
                                             'pct_districts_meeting_rounded', 'num_districts_meeting_rounded']]


# # save as csv
# os.chdir(GITHUB + '/''data/')
# df_districts_pct_all.to_csv("id6013_heatmap_easily_connected_districts.csv", index=False)

# # Heatmap: Percent of Districts "Easily Connected"
# df_input = df_districts_pct_all.copy()
# col_to_plot = 'pct_districts_meeting_rounded'
# plot_title = 'Percent of Districts Connected in 2019 and Can Be Easily Connected in 2023'
#
# #colorscale:
# scl = [[0.0, '#cccccc'],[0.2, '#bfe6ef'],[0.4, '#6acce0'],        [0.6, '#009296'],[0.8, '#006b6e'],[1.0, '#004f51']] # reds
#
# # create empty list for data object:
# data_slider = []
#
# # populate the data object
# for y in [2019, 2023]:
#     # select the year
#     df_year = df_input[df_input['year']== y]
#
#     # transform the columns into string type so I can:
#     for col in df_year.columns:
#         df_year[col+"_pp"] = df_year[col].astype(str)
#
#     df_year['text'] = 'Total Number of Districts: '+ df_year['district_counts_pp'] + '<br>' +    'Number of Districts Connected: ' + df_year['num_districts_meeting_rounded_pp']
#
#     # create the dictionary with the data for the current year
#     data_one_year = dict(
#                         type='choropleth',
#                         locations = df_year['state_code'],
#                         z=df_year[col_to_plot],
#                         locationmode='USA-states',
#                         colorscale = scl,
#                         text = df_year['text'],
#                         zmin=0,
#                         zmax=100.0,
#                         colorbar_title="Percent of Districts Connected",
#                         marker_line_color='white',
#                         colorbar_ticksuffix = '%',
#     )
#
#     data_slider.append(data_one_year)  # I add the dictionary to the list of dictionaries for the slider
#
# # create the steps for the slider
# steps = []
# for i in range(len(data_slider)):
#     if i == 0:
#         temp_year = 2019
#     else:
#         temp_year = 2019 + i + 2
#     step = dict(method='restyle',
#                 args=['visible', [False] * len(data_slider)],
#                 label=str(i + temp_year))  # label to be displayed for each step (year)
#     step['args'][1][i] = True
#     steps.append(step)
#
# # create the 'sliders' object from the 'steps'
# sliders = [dict(active=0, pad={"t": 1}, steps=steps, currentvalue = {"prefix": "Year: "})]
#
# # set up the layout (including slider option)
# layout = dict(geo=dict(scope='usa',
#                        projection={'type': 'albers usa'}),
#               title = plot_title,
#               sliders=sliders)
#
# # create the figure object:
# fig = dict(data=data_slider, layout=layout)
#
# # to plot in the notebook
# plotly.offline.iplot(fig)
#
# # to plot in a separete browser window
# offline.plot(fig, auto_open=True, image = 'png', image_filename="map_us_districts_pct_connected_092419" ,image_width=2000, image_height=1000,
#               filename='/Users/katherineaquino/Desktop/districts_pct_connected_slider_092419_final.html', validate=True);
#
#
#
# ### Heatmaps: Percent Students "Easily Connected"

# add students meeting 2019
df_sn_agg['num_students_meeting_2019'] = df_sn_agg.loc[:, 'num_students'] - df_sn_agg.loc[:, 'num_students_newly_meeting_extrap']

# 2019 students meeting
sub_cols = ['state_code', 'num_students_meeting_2019']
df_2019 = df_sn_agg[sub_cols]

# add year column
df_2019['year'] = [2019]*df_2019.shape[0]

# left join to add district counts
df_2019 = df_2019.merge(df_sn_agg[['state_code','num_students']], on='state_code')

# 2023 students meeting
sub_cols = ['state_code', 'num_students']
df_2023 = df_sn_agg[sub_cols]
df_2023.columns = ['state_code', 'num_students_meeting_2023']

# add year column
df_2023['year'] = [2023]*df_2023.shape[0]

# left join with district counts
df_2023 = df_2023.merge(df_sn_agg[['state_code','num_students']], on='state_code')

# set state as index
df_2023.set_index('state_code', inplace=True)
df_2019.set_index('state_code', inplace=True)

# substitute 'num_students_meeting_2019' for CT and ME
df_2023.loc['CT', 'num_students_meeting_2023'] = df_2019.loc['CT', 'num_students_meeting_2019']
df_2023.loc['ME', 'num_students_meeting_2023'] = df_2019.loc['ME', 'num_students_meeting_2019']

# reset index and rename
df_2023.reset_index(inplace=True)
df_2023.rename(columns={"num_students_meeting_2023": "num_students_meeting"}, inplace=True)

df_2019.reset_index(inplace=True)
df_2019.rename(columns={"num_students_meeting_2019": "num_students_meeting"}, inplace=True)

# concat dfs
df_students_pct = pd.concat([df_2019, df_2023]).reset_index(drop=True)

# divide for pct
df_students_pct['pct_students_meeting'] = (df_students_pct['num_students_meeting']/df_students_pct['num_students'])*100

# subset of 2023
df_students_pct_2023 = df_students_pct[df_students_pct.year == 2023]

sub_cols = ['state_code', 'num_students_meeting', 'year', 'num_students', 'pct_students_meeting']

# rename columns in df_districts_pct_js for concatenation
df_temp = df_districts_pct_js.rename(columns={'num_students_newly_meeting_extrap':'num_students_meeting',
                                              'num_students_meeting': 'num_students_meeting_orig'})

# adding hi
df_temp_hi = df_temp[df_temp.state_code == 'HI']
df_temp_hi.replace({'year': {2019: 2023}}, inplace=True)
df_temp_hi_subset = df_temp_hi[sub_cols]

# adding ms
df_temp_ms = df_temp[df_temp.state_code == 'MS']
df_temp_ms.replace({'year': {2019: 2023}}, inplace=True)
df_temp_ms_subset = df_temp_ms[sub_cols]

df_students_pct_all = pd.concat([df_students_pct_2023,
                                 df_temp[sub_cols],
                                 df_temp_hi_subset[sub_cols],
                                 df_temp_ms_subset[sub_cols]
                                ])

# round specific columns: num_students_meeting and pct_students_meeting
df_students_pct_all['pct_students_meeting_rounded'] = df_students_pct_all['pct_students_meeting'].round(0)
df_students_pct_all['num_students_meeting_rounded'] = df_students_pct_all['num_students_meeting'].round(0)

# format thousands as string
cols_to_pp = ['num_students','num_students_meeting_rounded']

def format_k_MM(row):
    if row >= 1000000:
        row_to_format = row/1000000
        return '{:,.1f}M'.format(row_to_format)
    else:
        row_to_format = row/1000
        return '{:,.0f}k'.format(row_to_format)

for col in cols_to_pp:
    df_students_pct_all[col+'_formatted'] = df_students_pct_all[col].apply(format_k_MM)


# rearrange columns and reset index
df_students_pct_all = df_students_pct_all[['state_code', 'year',
                                           'num_students', 'num_students_meeting',
                                           'pct_students_meeting', 'pct_students_meeting_rounded',
                                           'num_students_formatted', 'num_students_meeting_rounded',
                                           'num_students_meeting_rounded_formatted']]

# save as csv
#os.chdir(GITHUB + '/''data/')
df_students_pct_all.to_csv("id6014_heatmap_easily_connected_students.csv", index=False)


# # ### Heatmap: Percent of Students Easily Connected
#
# df_input = df_students_pct_all.copy()
# col_to_plot = 'pct_students_meeting_rounded'
# plot_title = 'Students Connected in 2019 and Can Be Easily Connected in 2023'
#
# #colorscale:
# scl = [[0.0, '#cccccc'],[0.2, '#fbdbca'],[0.4, '#f9a677'],        [0.6, '#f26c23'],[0.8, '#c44f27'],[1.0, '#83351a']] # reds
#
# # create empty list for data object:
# data_slider = []
#
# # populate the data object
# for y in [2019, 2023]:
#     # select the year
#     df_year = df_input[df_input['year']== y]
#
#     # transform the columns into string type so I can:
#     for col in df_year.columns:
#         df_year[col+"_pp"] = df_year[col].astype(str)
#
#     df_year['text'] = 'Total Number of Students: '+ df_year['num_students_formatted_pp'] + '<br>' +     'Number of Students Connected: ' + df_year['num_students_meeting_rounded_formatted_pp']
#
#     # create the dictionary with the data for the current year
#     data_one_year = dict(
#                         type='choropleth',
#                         locations = df_year['state_code'],
#                         z=df_year[col_to_plot],
#                         locationmode='USA-states',
#                         colorscale = scl,
#                         text = df_year['text'],
#                         zmin=0,
#                         zmax=100.0,
#                         colorbar_title="Percent of Students Connected",
#                         marker_line_color='white',
#                         colorbar_ticksuffix = '%'
#     )
#     data_slider.append(data_one_year)  # add the dictionary to the list of dictionaries for the slider
#
# # create the steps for the slider
# steps = []
# for i in range(len(data_slider)):
#     if i == 0:
#         temp_year = 2019
#     else:
#         temp_year = 2019 + i + 2
#     step = dict(method='restyle',
#                 args=['visible', [False] * len(data_slider)],
#                 label=str(i + temp_year))  # label to be displayed for each step (year)
#     step['args'][1][i] = True
#     steps.append(step)
#
# # create the 'sliders' object from the 'steps'
# sliders = [dict(active=0, pad={"t": 1}, steps=steps, currentvalue = {"prefix": "Year: "})]
#
# # set up the layout (including slider option)
# layout = dict(geo=dict(scope='usa',
#                        projection={'type': 'albers usa'}),
#               title = plot_title,
#               sliders=sliders)
#
# # create the figure object:
# fig = dict(data=data_slider, layout=layout)
#
# # to plot in the notebook
# plotly.offline.iplot(fig)
#
# # # to plot in a separete browser window
# offline.plot(fig, auto_open=True, image = 'png', image_filename="map_us_students_connected_092419_final" ,image_width=2000, image_height=1000,
#               filename='/Users/katherineaquino/Desktop/students_pct_connected_slider_092419_final.html', validate=True);
