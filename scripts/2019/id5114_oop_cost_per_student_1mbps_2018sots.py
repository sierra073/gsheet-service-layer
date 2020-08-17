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

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id5114_oop_cost_per_student_1mbps_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

data = pd.DataFrame(rows, columns=names)

queryfile = open('median_ia_annual_cost_per_student_national_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

national_median = pd.DataFrame(rows, columns=names)

myConnection.close()

data['median_ia_annual_cost_per_student'] = data['median_ia_annual_cost_per_student'].astype('float').round(2)
data = data.sort_values('median_ia_annual_cost_per_student')

plt.rc('figure', figsize=(6, 6))
rc('axes', edgecolor='#cccccc')
rcParams['font.sans-serif'] = ['Lato', 'sans-serif']
colors = ['#f26c23', '#fbdbca']
from matplotlib.font_manager import FontProperties
font = FontProperties()
font.set_weight('bold')
font.set_family('Lato')
font.set_size(12)

x = data['subgroup']
y = data['median_ia_annual_cost_per_student']
ymin = data[data.subgroup == 'Need to Spend More']['median_ia_annual_cost_per_student'] + 3.43
ymax = data[data.subgroup == 'Need to Spend More']['median_ia_annual_cost_per_student'] + 4.11

ax = plt.figure().gca()

rects = ax.bar(x, y, color=colors, width=.5068)
ax.bar(x[1], 4.11, bottom=y[1], color='#dd8668', hatch='////', edgecolor='#c44f27', width=.5)

ax.set_yticks([])
ax.tick_params(colors='#6d6d6d', labelsize='x-large')
ax.set_ylabel('Median Annual IA $/student', size='large', color='#6d6d6d')

ax.spines['top'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['right'].set_visible(False)


def autolabel(rects):
    """
    Attach a text label above each bar displaying its height
    """
    i = 0
    for rect in rects:
        if i == 1:  # comment out if want first one
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2., 1.02 * height,
                    '$' + str(int(round(height, 1))),
                    ha='center', va='bottom', size='x-large', color=colors[i], fontproperties=font)
        i += 1


autolabel(rects)
ax.set_xlim(-.93, 1.325)

plt.text(-.93, y[1], 'Current Spend: $' + str(int(math.ceil(y[1]))), color='#f26c23', alpha=.85, fontproperties=font)

plt.text(-.88, ymax - .2, '$3-4 more', color='#c44f27', alpha=.85, fontproperties=font)

# save image
os.chdir(GITHUB + '/''figure_images')
plt.savefig('id5114_oop_cost_per_student_1mbps_2018sots.png')
