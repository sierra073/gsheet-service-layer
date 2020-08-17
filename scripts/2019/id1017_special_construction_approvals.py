import os
import psycopg2
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.pyplot import gca
from matplotlib._png import read_png

# connection credentials
HOST = "charizard-psql1.cyttrh279zkr.us-east-1.rds.amazonaws.com"
USER = "eshadmin"
PASSWORD = "J8IkWgrwsxC&"
DB = "sots_snapshot_2019_08_27"
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER,
                                password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id1017_special_construction_approvals.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

data = pd.DataFrame(rows, columns=names)
myConnection.close()

sp_approvals = data

fig, ax = plt.subplots()

frame1 = plt.gca()

prop = fm.FontProperties()
prop.set_family('Lato')

sns.set_style('white', {'axes.linewidth': .5, 'axes.edgecolor':'#A1A1A1'})
sns.despine(left=True, right=True)

#line for funded requests
plt.plot(sp_approvals.funding_year, sp_approvals.frns_funded,
         color="#256b01", lw = 1.5, zorder=1, label='Funded')


#scatter for funded requests
plt.scatter(sp_approvals.funding_year, sp_approvals.frns_funded,
            color="#9fce87", zorder=3, s=100, label='_nolegend_')

#line for denied requests
plt.plot(sp_approvals.funding_year, sp_approvals.frns_denied,
         color="#7c250f", lw = 1, zorder=2, label='Denied')

#scatter for denied requests
plt.scatter(sp_approvals.funding_year, sp_approvals.frns_denied,
            color="#cea68f", zorder=4, s=100, label='_nolegend_')
#crating legend
plt.legend(framealpha=1, frameon=True)

#x axis
plt.xticks(sp_approvals.funding_year)
for label in ax.get_xticklabels() :
    label.set_fontproperties(prop)
ax.tick_params(axis='x', colors='#A1A1A1')

#y axis
plt.ylim([0,300])
for label in ax.get_yticklabels() :
    label.set_fontproperties(prop)
ax.tick_params(axis='y', colors='#A1A1A1')
#yaxis label
plt.ylabel('Requests for Special Construction', color="#A1A1A1",
           fontproperties=prop,size=14)
#chart title
plt.title("More special construction \nprojects are being funded",
          fontproperties=prop, size=14)

plt.savefig(GITHUB+'/''figure_images/id1017_special_construction_approvals.png')
