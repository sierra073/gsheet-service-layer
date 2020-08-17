import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from matplotlib.offsetbox import AnchoredOffsetbox, TextArea, HPacker, VPacker
plt.rc('figure', figsize=(10, 7))

import seaborn as sns

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id5017_dec_total_cost_ufiber_2018sots.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

myConnection.close()

ufiber = pd.DataFrame(rows, columns=names)

ufiber = ufiber.astype('float')

x1 = [2018, 2019]
y1 = [ufiber['median_ia_monthly_cost_per_mbps_17'].item(), ufiber['median_ia_monthly_cost_per_mbps_18'].item()]

x2 = x1
y2 = [ufiber['median_ia_monthly_cost_total_17'].item(), ufiber['median_ia_monthly_cost_total_18'].item()]

ax = plt.figure().gca()
ax2 = ax.twinx()
plt.suptitle('Cost per mbps and total cost trends for districts that upgraded to fiber (2018-2019) ')
ax.xaxis.set_major_locator(MaxNLocator(integer=True))
a, = ax.plot(x1, y1, 'bo-', color='seagreen', label='cost per mbps')
b, = ax2.plot(x2, y2, 'bo-', color='#80bec9', label='total cost')
ax.set_ylim(0, 10)
ax2.set_ylim(0, 2500)
plt.xlabel('Funding Year')
plt.ylabel('Cost ($)')

p = [a, b]
ax.legend(p, [p_.get_label() for p_ in p],
          loc=(0, 0), fontsize='small')

# save image
os.chdir(GITHUB + '/''figure_images')
plt.savefig('id5029_dec_cost_ufiber_fig_2018sots.png')
