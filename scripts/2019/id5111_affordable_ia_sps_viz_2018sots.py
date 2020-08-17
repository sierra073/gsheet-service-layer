import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

import seaborn as sns
from matplotlib import rcParams


HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

# connect to dar and save list of all clean for cost districts (excl. AK) with all of the relevant metrics/indicators
myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
queryfile = open('id5111_affordable_ia_sps_viz_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
sp_data = pd.DataFrame(rows, columns=names)

sp_data['deals_rank'] = sp_data.num_deals.rank()
sp_data = sp_data.sort_values(by='num_students').reset_index()

sp_data['deals_rank2'] = sp_data['deals_rank']**1.5 + 30

# Create a dataframe
sp_df = sp_data[['primary_sp', 'num_students']]
sp_df['num_students'] = sp_df['num_students'] / sum(sp_df['num_students'])

# Reorder it following the values:
ordered_df = sp_df.sort_values(by='num_students')
my_range = range(1, len(sp_df.index) + 1)

plt.rc('figure', figsize=(11, 7))
rcParams.update({'figure.autolayout': True})

my_color = '#fdb913'
import matplotlib.colors as clr
cmap = clr.LinearSegmentedColormap.from_list('custom yellow', ['#fff6e2', '#fdb913'], N=28)
my_size = sp_data['deals_rank2']

plt.hlines(y=my_range, xmin=0, xmax=ordered_df['num_students'], color=my_color, alpha=0.4)
plt.scatter(ordered_df['num_students'], my_range, c=my_size, cmap=cmap, s=my_size, alpha=1)

# Add title and exis names
plt.yticks(my_range, ordered_df['primary_sp'])
plt.xlabel('Percent of Students')
plt.gca().set_xticklabels(['{:.0f}%'.format(x * 100) for x in plt.gca().get_xticks()])

# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id5111_affordable_ia_sps_viz_2018sots.png')
