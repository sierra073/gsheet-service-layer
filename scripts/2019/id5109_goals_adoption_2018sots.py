import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
plt.rc('figure', figsize=(10, 7))

import seaborn as sns

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

# connect to dar and save list of all clean for cost districts (excl. AK) with all of the relevant metrics/indicators
myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
queryfile = open('id5109_100k_adoption_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
a100k = pd.DataFrame(rows, columns=names)

a100k['funding_year'] = a100k['funding_year'].astype('int')
a100k['num_students'] = a100k['num_students'].astype('float')


queryfile = open('id5109_1mbps_adoption_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
a1mbps = pd.DataFrame(rows, columns=names)

a1mbps['funding_year'] = a1mbps['funding_year'].astype('int')
a1mbps['num_students'] = a1mbps['num_students'].astype('float')

sns.set_style("white")
from scipy.interpolate import spline

# 47086264 = 2019 current students

# Create data and smooth
x = a100k['funding_year']
y = a100k['num_students']
x_smooth = np.linspace(x.min(), x.max(), 200)
y_smooth = spline(x, y, x_smooth)
x2 = a1mbps['funding_year']
y2 = a1mbps['num_students']
x2_smooth = np.linspace(x2.min(), x2.max(), 200)
y2_smooth = spline(x2, y2, x2_smooth)

# Make the plot
fig, ax = plt.subplots()
a, = ax.plot(x_smooth, y_smooth, color='#fdb913', lw=3, label='100 kbps/student')
b, = ax.plot(x2_smooth, y2_smooth, color='#f09222', lw=3, label='1 Mbps/student')
ax.fill_between(x_smooth, 0, y_smooth, color='#fdb913', alpha=.3)
ax.fill_between(x2_smooth, 0, y2_smooth, color='#f09222', alpha=.4)

plt.xlabel('Funding Year')
plt.ylabel('Percent of Students Newly Meeting*')

# manipulate
vals = ax.get_yticks()
ax.set_yticklabels(['{:.0f}%'.format(x * 100) for x in vals])

p = [a, b]
ax.legend(p, [p_.get_label() for p_ in p],
          loc='upper right', fontsize='medium')

yp = max(y2)
ax.axhline(y=yp, color='grey', linestyle='--')

# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id5109_goals_adoption_2018sots.png')
