##imports and definitions
from __future__ import division
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta


HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

# connect to dar and save list of all clean for cost districts (excl. AK) with all of the relevant metrics/indicators
myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id5016_expiring_multi_year_contract_lengths.sql', 'r')
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

df['contract_len_rounded'] = df.contract_len.map(round)

counts = df[['district_id', 'contract_len_rounded']].groupby('contract_len_rounded').count().reset_index()
counts['percent'] = counts.district_id.map(lambda x: round(x/counts.district_id.sum()*100, 1))

plt.figure(figsize=(12, 7))
xmarks = np.arange(0, counts.shape[0])
plt.xticks(xmarks, counts.contract_len_rounded)
plt.yticks([])
plt.bar(xmarks, counts.district_id)
plt.xlabel('Contract Length in Years (rounded)')
plt.ylabel('Number/% of Districts')
plt.box(on=None)

for x0, y0, label, pct_label in zip(xmarks, counts.district_id, counts.district_id, counts.percent):
    plt.text(x0, y0, str(label), ha='center', va='bottom', color='orange', weight='bold')
    plt.text(x0, y0+(counts.district_id.max()/30), str(pct_label)+'%', ha='center', va='bottom', weight='bold')

os.chdir(GITHUB + '/''figure_images/')
plt.savefig('id5016_hist_multi_year_contract_len.png')
