import psycopg2
import os
import pandas as pd
import numpy as np
from sklearn import preprocessing
import matplotlib.pyplot as plt
plt.rc("font", size=11)
plt.rc('figure', figsize=(11, 7))

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

locale_stack = pd.crosstab(df.no_decrease_no_aff, df['size']).apply(lambda r: r / r.sum(), axis=1).reset_index()
locale_stack = locale_stack[['no_decrease_no_aff', 'Mega', 'Large', 'Medium', 'Small', 'Tiny']]

# Values of each group
bars1 = locale_stack['Mega']
bars2 = locale_stack['Large']
bars3 = locale_stack['Medium']
bars4 = locale_stack['Small']
bars5 = locale_stack['Tiny']

# The position of the bars on the x-axis
r = [0, 1]
# Names of group and bar width
names = ['False', 'True']
barWidth = 1

# 1
plt.bar(r, bars1, color='#fedbcc', edgecolor='white', width=barWidth, label='Mega')
# 2
plt.bar(r, bars2, bottom=bars1, color='#fcaf93', edgecolor='white', width=barWidth, label='Large')
# 3
plt.bar(r, bars3, bottom=bars1 + bars2, color='#fc8161', edgecolor='white', width=barWidth, label='Medium')
# 4
plt.bar(r, bars4, bottom=bars1 + bars2 + bars3, color='#f44f39', edgecolor='white', width=barWidth, label='Small')
# 5
plt.bar(r, bars5, bottom=bars1 + bars2 + bars3 + bars4, color='#d52221', edgecolor='white', width=barWidth, label='Tiny')

# Custom X axis
plt.xticks(r, names, fontweight='bold')
plt.xlabel("No Cost Decrease, Not Meeting Benchmark Prices")
plt.legend(loc="upper left", bbox_to_anchor=(1, 1), ncol=1)

# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id5028_no_aff_size_2018sots.png')
