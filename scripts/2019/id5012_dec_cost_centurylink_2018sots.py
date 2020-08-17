import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
plt.rc("font", size=15)
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

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id5003_districts_cost_decrease_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

success = None
print ("Trying to establish initial connection to the server")
while success is None:
    conn = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DB)
    cur = conn.cursor()
    try:
        cur.execute(query)
        print("Success!")
        success = "true"
        break
    except psycopg2.DatabaseError:
        print('Server closed connection, trying again')
        pass
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=names)

myConnection.close()

df['pchg_cost_mbps'] = df['pchg_cost_mbps'].astype('float')

# define "served by CenturyLink"
df['sp_centurylink'] = np.where(df.primary_sp == 'CenturyLink', 'CenturyLink', 'Other')

# construct figure
sns.set(style="white", color_codes=True)
x = pd.crosstab(df['sp_centurylink'], df.cost_decrease_indicator).apply(lambda r: r / r.sum(), axis=1).reset_index()
vec = x[True].rename({0: 'CenturyLink', 1: 'Other'}).astype('float')
ax = vec.plot(kind='bar', legend=None, color=['seagreen', 'steelblue'])

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
plt.xlabel('Service Provider')
plt.ylabel('% Districts with Cost Decrease')
plt.yticks([])
plt.xticks(rotation='horizontal')
# set individual bar lables using above list
j = 0
for i in ax.patches:
    # get_x pulls left or right; get_height pushes up or down
    ax.text(i.get_x() + .18, i.get_height() + .01,
            str(round(vec[j] * 100, 1)) + '%', fontsize=12)
    j += 1

# save image
os.chdir(GITHUB + '/''figure_images')
plt.savefig('id5012_dec_cost_centurylink_2018sots.png')
