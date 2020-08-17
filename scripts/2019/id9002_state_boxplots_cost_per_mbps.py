import psycopg2
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)
cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id9002_state_boxplots_cost_per_mbps.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
data = pd.DataFrame(rows, columns=names)
data['ia_monthly_cost_per_mbps'] = data['ia_monthly_cost_per_mbps'].astype(float)
# filter to 6 states
data = data[data.state_code.isin(['AL', 'KY', 'NC', 'RI', 'TN', 'WV'])]

ax = sns.boxplot(x="state_code", y="ia_monthly_cost_per_mbps", data=data, linewidth=1.5)

os.chdir(GITHUB + '/''figure_images')
plt.savefig('id9002_state_boxplots_cost_per_mbps.png')
