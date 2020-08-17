import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rc, rcParams, font_manager
from matplotlib.ticker import MaxNLocator
from matplotlib.offsetbox import AnchoredOffsetbox, TextArea, HPacker, VPacker

import seaborn as sns

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
queryfile = open('median_ia_annual_cost_per_student_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

myConnection.close()

data = pd.DataFrame(rows, columns=names)

data['median_ia_annual_oop_per_student'] = data['median_ia_annual_oop_per_student'].astype('float').round(2)
data = pd.concat([data[data.subgroup != 'Meeting 100kbps Goal'], data[data.subgroup == 'Meeting 100kbps Goal']])

plt.rc('figure', figsize=(8, 7))
rc('axes', edgecolor='#cccccc')
rcParams['font.sans-serif'] = ['Lato', 'sans-serif']

x = data.reset_index(drop=True)['subgroup']
y = data['median_ia_annual_oop_per_student']


ax = plt.figure().gca()

rects = ax.bar(x.index, y, color=['#fdb913', '#d19328', '#956423'], width=.5)


ax.set_yticks([])
ax.set_ylabel('Median Annual OOP $/student', size='large', color='#6d6d6d')

ax.set_xticks(x.index.tolist(), x.tolist())
ax.tick_params(colors='#6d6d6d', labelsize='x-large')


ax.spines['top'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['right'].set_visible(False)


def autolabel(rects):
    """
    Attach a text label above each bar displaying its height
    """
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2., 1.02 * height,
                '$' + str(height),
                ha='center', va='bottom', size='large', color='#6d6d6d')


autolabel(rects)

# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id6039_oop_per_student_increase_budget_bar_2018sots.png')
