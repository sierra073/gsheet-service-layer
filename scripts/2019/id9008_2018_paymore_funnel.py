from __future__ import division
import psycopg2
import os
import pandas as pd

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)
cur = myConnection.cursor()

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
queryfile = open('id9008_2018_paymore_funnel.sql', 'r')
query = queryfile.read()
queryfile.close()

# get 2019 funnel
cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
data = pd.DataFrame(rows, columns=names)

queryfile = open('id9008_2018_extrapolation_notmeeting.sql', 'r')
query = queryfile.read()
queryfile.close()

# get 2018 extrapolated population counts
cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
data_exp = pd.DataFrame(rows, columns=names)
data_exp['num_districts'] = data_exp['num_districts'].astype('float')
extrap = data_exp[data_exp.path_to_meet_2018_goal_group == 'Pay More']['num_districts'].values[0]

data['nd'] = data['nd'].astype('int')
total = data[data.status_2019 == "Total"]['nd'].values.tolist()
data = data[data.status_2019 != "Total"].sort_values('nd').reset_index(drop=True)
vals_to_plot = ((data['nd'].iloc[[1, 0, 2]]/total[0])*extrap).astype('int').reset_index(drop=True)

# save csv - NEEDED FOR ISL
os.chdir(GITHUB + '/Projects/sots-isl/data')
vals_to_plot.to_csv("id9008_2018_paymore_funnel.csv", index=False)
