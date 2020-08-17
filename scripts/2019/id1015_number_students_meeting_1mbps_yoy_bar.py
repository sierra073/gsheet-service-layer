import os
import psycopg2
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import rc, rcParams, font_manager
from matplotlib.ticker import MaxNLocator
from matplotlib.offsetbox import AnchoredOffsetbox, TextArea, HPacker, VPacker

# connection credentials
HOST = os.environ.get("HOST_DAR_PROD")
USER = os.environ.get("USER_DAR_PROD")
PASSWORD = os.environ.get("PASSWORD_DAR_PROD")
DB = os.environ.get("DB_DAR_PROD")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER,
                                password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id101415_num_students_meeting_1mbps.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

data = pd.DataFrame(rows, columns=names)
myConnection.close()

df_results = data

plt.rc('figure', figsize=(8, 7))
rc('axes', edgecolor='#cccccc')
rcParams['font.sans-serif'] = ['Lato', 'sans-serif']

x = df_results['funding_year']
y = df_results['num_students_meeting_ext']


ax = plt.figure().gca()

rects = ax.bar(x, y, color=['#f2b728','#f2b728','#f2b728','#f2b728','#f26c23'], width=.5)
ax.set_yticks([])
ax.tick_params(colors='#6d6d6d', labelsize='x-large')
ax.set_ylabel('# of Students meeting 1mbps', size='large', color='#6d6d6d')

ax.spines['top'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['right'].set_visible(False)


def autolabel(rects):
    """
    Attach a text label above each bar displaying its height
    """
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, 1 * height,
                str(height)+'M',
                ha='center', va='bottom', size='large', color='#6d6d6d')


autolabel(rects)

plt.savefig(GITHUB+'/''figure_images/id1015_number_students_meeting_1mbps_yoy_bar.png')
