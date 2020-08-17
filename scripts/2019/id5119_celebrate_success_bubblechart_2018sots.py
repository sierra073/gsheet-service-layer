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

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
queryfile = open('celebrate_success_bubblechart_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

data = pd.DataFrame(rows, columns=names)
myConnection.close()

data[['pct_districts_meeting', 'pct_schools_meeting', 'pct_campuses_meeting', 'pct_students_meeting', 'pct_schools_on_fiber']] = data[['pct_districts_meeting', 'pct_schools_meeting', 'pct_campuses_meeting', 'pct_students_meeting', 'pct_schools_on_fiber']].astype('float')
data['num_students'] = data['num_students'].astype('int64')
data = data[data.pct_schools_on_fiber >= .8]  # removing a few states to show fiber movement more

plt.rc('figure', figsize=(14, 7))
rc('axes', edgecolor='#a5a5a5')
rcParams['font.sans-serif'] = ['Lato', 'sans-serif']

from matplotlib.font_manager import FontProperties
font = FontProperties()
font.set_weight('bold')
font.set_family('Lato')
font.set_size(14)


def plot_bubblechart(col):
    s1 = data[['funding_year', 'num_students', col, 'pct_schools_on_fiber']].sort_values(['funding_year', 'num_students'], ascending=[True, False])
    s1_sep = [s1[s1.funding_year == 2015][['num_students', col, 'pct_schools_on_fiber']],
              s1[s1.funding_year == 2019][['num_students', col, 'pct_schools_on_fiber']]]

    fig, ax = plt.subplots(1, 2, sharex=True, sharey=True)

    for j in range(2):
        x = s1_sep[j]['pct_schools_on_fiber'] * 100
        y = s1_sep[j][col] * 100
        st = s1_sep[j]['num_students']
        ax[j].xaxis.set_major_locator(MaxNLocator(integer=True))

        ax[j].scatter(x, y, s=st / 1000, c="#009296", alpha=0.5, edgecolor='#006b6e', linewidth=1.32)
        if j == 0:
            ax[j].set_title('2015', size=14)
            ax[j].set_ylabel('% Schools Meeting 100 kbps', size=12)
            ax[j].set_xlabel('% Schools on Fiber', size=12)
        else:
            ax[j].set_title('2019', size=14)


plot_bubblechart('pct_campuses_meeting')

# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id5119_celebrate_success_bubblechart_2018sots.png')
