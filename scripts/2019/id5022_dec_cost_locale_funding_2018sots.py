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

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
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
df['ia_annual_cost_erate'] = df['ia_annual_cost_erate'].astype('float')
df['ia_funding_requested_erate'] = df['ia_funding_requested_erate'].astype('float')

# group locale and compute cost ratio metrics
df['locale_grouped'] = np.where(np.logical_or(df['locale'] == 'Rural', df['locale'] == 'Town'), 'Rural', df['locale'])
locale_summ = df[['locale_grouped', 'num_students', 'ia_annual_cost_erate', 'ia_funding_requested_erate']].groupby('locale_grouped').sum().reset_index()

locale_summ['ia_annual_cost_per_student'] = locale_summ['ia_annual_cost_erate'] / locale_summ['num_students']
locale_summ['ia_annual_funding_per_student'] = locale_summ['ia_funding_requested_erate'] / locale_summ['num_students']
locale_summ['cost_vs_funding'] = locale_summ['ia_annual_cost_per_student'] / locale_summ['ia_annual_funding_per_student']

locale_summ = pd.concat([locale_summ[locale_summ.locale_grouped == 'Suburban'], locale_summ[locale_summ.locale_grouped != 'Suburban']])

locale_summ = locale_summ.set_index('locale_grouped')

# construct figure
sns.set(style="whitegrid", color_codes=True)
plt.rc("font", size=15)
plt.rc('figure', figsize=(10, 7))

ax = locale_summ[['ia_annual_cost_per_student', 'ia_annual_funding_per_student']].plot(kind='bar', colors=['#0b559f', '#539ecd'])
plt.xlabel('Locale (Rural/Town Grouped)')
plt.ylabel('Cost ($)')
plt.xticks(rotation='horizontal')

rects = ax.patches

# Make some labels.
labels = locale_summ['cost_vs_funding'].round(2).astype("str") + " x"
labels = labels.tolist()

for rect, label in zip(rects, labels):
    height = rect.get_height()
    ax.text((rect.get_x()) + rect.get_width() / 2, height + .5, label,
            ha='center', va='bottom')

# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id5022_dec_cost_locale_funding_2018sots.png')
