import psycopg2
import os
import pandas as pd
import numpy as np
from sklearn import preprocessing
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

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
queryfile = open('id5005_districts_more_money_2018sots.sql', 'r')
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

# construct figure
sns.set(style="white", color_codes=True)
x = pd.crosstab(df.no_decrease_no_aff, df.frns_received_0_bids).apply(lambda r: r / r.sum(), axis=1).reset_index()
x = pd.concat([x[x.no_decrease_no_aff == True], x[x.no_decrease_no_aff == False]])
vec = x[True].rename({1: 'True', 0: 'False'}).astype('float')
ax = vec.plot(kind='bar', legend=None, color=['#da3b46', 'grey'])

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
plt.xlabel('No Cost Decrease, Not Meeting Benchmark')
plt.ylabel('% Districts Received 0 bids on their FRN(s)')
plt.yticks([])
plt.xticks(rotation='horizontal')
# set individual bar lables using above list
j = 0
for i in ax.patches:
    # get_x pulls left or right; get_height pushes up or down
    ax.text(i.get_x() + .17, i.get_height() + .001,
            str(round(vec[j] * 100, 1)) + '%', fontsize=12)
    j += 1

# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id5027_no_aff_0bid_2018sots.png')