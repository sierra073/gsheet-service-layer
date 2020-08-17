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
queryfile = open('id5107_oop_cost_per_student_frl_2018sots.sql', 'r')
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

df['frl_percent'] = df['frl_percent'].astype('float').round(0)
df['ia_annual_oop_cost_per_student'] = df['ia_annual_oop_cost_per_student'].astype('float').round(2)

df['frl_percent_bucket'] = np.where(df['frl_percent'] <= 10, 10,
                                    np.where(df['frl_percent'] <= 20, 20,
                                             np.where(df['frl_percent'] <= 30, 30,
                                                      np.where(df['frl_percent'] <= 50, 50,
                                                               np.where(df['frl_percent'] <= 70, 70,
                                                                        np.where(df['frl_percent'] <= 80, 80, 90))))))


dfg = df[['subgroup', 'frl_percent_bucket', 'ia_annual_oop_cost_per_student']].groupby(['subgroup', 'frl_percent_bucket']).median().reset_index()

dfg_final = dfg[dfg.subgroup == 'meeting 1 Mbps'].merge(dfg[dfg.subgroup != 'meeting 1 Mbps'], on='frl_percent_bucket')

dfg_final = dfg_final[['frl_percent_bucket', 'ia_annual_oop_cost_per_student_x', 'ia_annual_oop_cost_per_student_y']]
dfg_final.columns = ['frl_percent_bucket', 'meeting_1mbps', 'need_to_spend_more']

vec2 = dfg_final.set_index('frl_percent_bucket')

sns.set(style="white", color_codes=True)
#'#da3b46'
ax = vec2.plot(kind='bar', color=['steelblue', '#da3b46'])

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
plt.xlabel('FRL Percent')
plt.title('Median Annual $/student for Internet Access \n by FRL Percent', size='large')
plt.yticks([])
plt.xticks(rotation='horizontal')
# set individual bar lables using above list
for p in ax.patches:
    ax.annotate('$' + str(round(p.get_height(), 2)), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center', va='center', xytext=(0, 10), textcoords='offset points')

# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id5107_oop_cost_per_student_frl_2018sots.png')
