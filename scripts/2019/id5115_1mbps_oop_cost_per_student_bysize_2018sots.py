import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rc, rcParams, font_manager
from matplotlib.ticker import MaxNLocator
from matplotlib.offsetbox import AnchoredOffsetbox, TextArea, HPacker, VPacker
import math

import seaborn as sns

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('ia_annual_cost_per_student_1mbps_districts_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

success = None
print("Trying to establish initial connection to the server")
while success is None:
    try:
        myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)
        cur = myConnection.cursor()
        cur.execute(query)
        print("Success!")
        success = "true"
        break
    except psycopg2.DatabaseError:
        print('Server closed connection, trying again')
        pass

names = [x[0] for x in cur.description]
rows = cur.fetchall()

data = pd.DataFrame(rows, columns=names)
myConnection.close()


data[['ia_annual_cost_per_student', 'oop_cost', 'num_students']] = data[['ia_annual_cost_per_student', 'oop_cost', 'num_students']].astype('float')

data = data.drop('district_id', axis=1)

high = .975
quant_df = data['ia_annual_cost_per_student'].quantile([high])

data = data[data.ia_annual_cost_per_student < quant_df.item()]

# g = sns.FacetGrid(data[['size','ia_annual_cost_per_student']], col="size")
# g = g.map(plt.hist, "ia_annual_cost_per_student")

data_median = data[['size', 'ia_annual_cost_per_student']].groupby('size').median().reset_index()
data_median.columns = ['size', 'median_ia_annual_cost_per_student']

datasum = data.groupby('size').sum().reset_index()
datasum['wtavg_ia_annual_cost_per_student'] = datasum['oop_cost'] / datasum['num_students']

data_wtavg = datasum[['size', 'wtavg_ia_annual_cost_per_student']]

data_final = data_median.merge(data_wtavg, on='size')

wtavg = data['oop_cost'].sum() / data['num_students'].sum()
med = data['ia_annual_cost_per_student'].median()

temp = pd.DataFrame({'size': 'Overall',
                     'median_ia_annual_cost_per_student': med,
                     'wtavg_ia_annual_cost_per_student': wtavg}, index=[0])

data_final = pd.concat([data_final, temp], ignore_index=True)
data_final = data_final[['size', 'median_ia_annual_cost_per_student', 'wtavg_ia_annual_cost_per_student']]

# save to_csv
os.chdir(GITHUB + '/''data')
data_final.to_csv("id5115_1mbps_oop_cost_per_student_bysize_2018sots.csv", index=False)
