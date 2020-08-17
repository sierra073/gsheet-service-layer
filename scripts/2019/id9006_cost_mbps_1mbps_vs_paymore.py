import psycopg2
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rc

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)
cur = myConnection.cursor()

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
queryfile = open('id9006_cost_mbps_1mbps_vs_paymore.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
data = pd.DataFrame(rows, columns=names)


data_plot_num = data[data.metric == 'median'][['meeting_2018_goal_oversub', 'ia_monthly_cost_per_mbps']]
data_plot_num['ia_monthly_cost_per_mbps'] = data_plot_num['ia_monthly_cost_per_mbps'].astype(float)
data_plot_num = data_plot_num.sort_values('ia_monthly_cost_per_mbps', ascending=False)

rc('figure', figsize=(8, 8))
rc('axes', edgecolor='#cccccc')
font = {'family': 'Lato', 'size': 14}
rc('font', **font)

ax = data_plot_num.plot(x='meeting_2018_goal_oversub', kind='bar', stacked=True, color="#009296", alpha=0.8, legend=False)
ax.patches[0].set_facecolor("#6acce0")
ax1 = plt.axes()
ax.set_yticks([])
ax.set_yticklabels([])
ax.set_ylabel('Median Cost per Mbps')
ax.set_xlabel('')
ax.set_xticklabels(data_plot_num['meeting_2018_goal_oversub'], rotation=0)


# add labels
for p in ax.patches:
    width, height = p.get_width(), p.get_height()
    x, y = p.get_xy()
    ax.text(x+width/2,
            y+height+(width/12.5),
            '${:,.2f}'.format(height),
            horizontalalignment='center',
            verticalalignment='center')

os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id9006_cost_mbps_1mbps_vs_paymore.png')
