import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rc, rcParams, font_manager
from matplotlib.ticker import MaxNLocator
from matplotlib.offsetbox import AnchoredOffsetbox, TextArea, HPacker, VPacker

import seaborn as sns

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/Projects/sots-isl/scripts/')
queryfile = open('id2019_states_progress_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()


cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

all_data = pd.DataFrame(rows, columns=names)
all_data = all_data.astype('float')

all_data = all_data[(all_data.funding_year == 2015) | (all_data.funding_year == 2019)]

conn1 = all_data[all_data.threshold == .9]['state_connectivity']
conn2 = all_data[all_data.threshold == .99]['state_connectivity']
fiber = all_data[all_data.threshold == .99]['state_fiber']
wifi = all_data[all_data.threshold == .65]['state_wifi']


def make_line_slope(data):
    plt.rc('figure', figsize=(8, 7))
    x = [2015, 2019]
    y = [data.iloc[0], data.iloc[1]]

    rc('axes', edgecolor='#cccccc')
    rcParams['font.sans-serif'] = ['Lato', 'sans-serif']

    ax = plt.figure().gca()

    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    a, = ax.plot(x, y, 'bo-', color='#009296', linewidth=2.5)
    ax.set_yticks([])
    ax.set_xticks(x)

    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_position(('axes', .03))
    ax.spines['right'].set_position(('axes', .98))

    ax.tick_params(colors='#cccccc', size=0)
    ax.xaxis.set_tick_params(labelsize=17)

    for i, j in zip(x, y):
        ax.annotate(str(int(j)), xy=(i - .03, j + .8), color='#009296', size=20)


def make_bar(data):
    plt.rc('figure', figsize=(5, 7))
    x = [2015, 2019]
    y = [data.iloc[0], data.iloc[1]]

    rc('axes', edgecolor='#cccccc')
    rcParams['font.sans-serif'] = ['Lato', 'sans-serif']

    ax = plt.figure().gca()

    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.bar(x, y, color=['#bfe6ef', '#009296'], width=1.5)
    ax.set_yticks([])
    ax.set_xticks(x)
    ax.xaxis.set_tick_params(labelsize=7)

    ax.spines['top'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['right'].set_visible(False)

    ax.tick_params(colors='#cccccc', size=0)
    ax.xaxis.set_tick_params(labelsize=17)

    for i, j in zip(x, y):
        ax.annotate(str(int(j)), xy=(i - .11, j + .3), color='#009296', size=20)


# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
make_line_slope(conn1)
plt.savefig('id5112_conn1_line.png')
make_bar(conn1)
plt.savefig('id5112_conn1_bar.png')
make_line_slope(fiber)
plt.savefig('id5112_fiber_line.png')
make_bar(fiber)
plt.savefig('id5112_fiber_bar.png')
make_line_slope(wifi)
plt.savefig('id5112_wifi_line.png')
make_bar(wifi)
plt.savefig('id5112_wifi_bar.png')
