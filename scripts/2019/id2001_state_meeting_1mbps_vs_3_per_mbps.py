import psycopg2
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rc, rcParams
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import MaxNLocator

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id2001_state_meeting_1mbps_and_afford_percents.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

data = pd.DataFrame(rows, columns=names)
myConnection.close()

data[['perc_meeting_affordability','perc_meeting_1mbps']] = data[['perc_meeting_affordability','perc_meeting_1mbps']].astype('float')

plt.rc('figure', figsize=(7, 7))
rcParams['text.color'] = 'black'
fig, ax = plt.subplots()

x = data['perc_meeting_1mbps']*100
y = data['perc_meeting_affordability']*100

ax.plot(x,y,c="#2980B9",alpha=0.5, marker = 'o', markeredgecolor='#1F618D', ls ='none')
ax.set(xlabel = '% Districts Meeting 1Mbps Goal', ylabel = '% Districts Under $3/Mbps', title='Poor affordability states also struggle with 1Mbps')
ax.axvline(x=50,linewidth=2,alpha=0.3,c='k',linestyle='dotted')
ax.axhline(y=50,linewidth=2,alpha=0.3,c='k',linestyle='dotted')


# save image
os.chdir(GITHUB + '/''figure_images')
fig.savefig('id2001_state_meeting_1mbps_vs_3_per_mbps.png')
