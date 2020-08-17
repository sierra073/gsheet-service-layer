import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

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
queryfile = open('id5110_affordable_ia_reimbursement_rate_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
data = pd.DataFrame(rows, columns=names)

data['pct_districts'] = data['pct_districts'].astype('float') * 100
data['c1_discount_rate'] = data['c1_discount_rate'].astype('int')

x1 = data[data.subgroup == 'districts']['c1_discount_rate']
y1 = data[data.subgroup == 'districts']['pct_districts']
x2 = data[data.subgroup == 'peers']['c1_discount_rate']
y2 = data[data.subgroup == 'peers']['pct_districts']

ax = plt.figure().gca()
plt.title("C1 Discount Rate Distribution:\n Districts not meeting that couldn't afford upgrades vs. their peers")

a, = ax.plot(x1, y1, '-', lw=3, color='#cb2128', label='districts')
b, = ax.plot(x2, y2, '-', lw=3, color='grey', label='peers')
ax.fill_between(x1, y1, y2, where=np.array(y1) >= np.array(y2), facecolor='#cb2128', alpha=0.2, interpolate=True)
# ax.set_ylim(0, 10)
# ax2.set_ylim(0, 2500)
ax.set_xlabel('C1 Discount Rate (%)')
ax.set_ylabel('Percent of Districts (%)')

p = [a, b]
ax.legend(p, [p_.get_label() for p_ in p],
          loc='upper left', fontsize='medium')

# save image
os.chdir(GITHUB + '/''figure_images')
plt.savefig('id5110_affordable_ia_reimbursement_rate_2018sots.png')
