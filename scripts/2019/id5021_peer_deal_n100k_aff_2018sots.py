import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredOffsetbox, TextArea, HPacker, VPacker
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
queryfile = open('id5020_peer_deals_summ_2018sots.sql', 'r')
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

df_sub = df[df.meeting_2014_goal_no_oversub == False]

x2 = pd.crosstab(df_sub['had_peer_deal'], df_sub.dec_cost_more_bw).apply(lambda r: r / r.sum(), axis=1).reset_index()
x2.columns = ['had_peer_deal', 'no_cost_decrease', 'cost_decrease']

sns.set(style="white")
peer_deal = ['No Peer Deal', 'Had Peer Deal']
cost_decrease = np.array(x2['cost_decrease'])
no_cost_decrease = np.array(x2['no_cost_decrease'])
ind = [i for i, _ in enumerate(peer_deal)]

ax = plt.gca()

p1 = plt.bar(ind, no_cost_decrease, width=0.6, label='No Cost/Mbps Decrease', color='#da3b46', edgecolor='white', linewidth=.7, bottom=cost_decrease)
p2 = plt.bar(ind, cost_decrease, width=0.6, label='Cost/Mbps Decrease', color='#3f7f93', edgecolor='white', linewidth=.7)

ybox1 = TextArea("No Cost/Mbps Decrease", textprops=dict(color="#da3b46", size=15, rotation=90, ha='left', va='bottom'))
ybox2 = TextArea("Cost/Mbps Decrease    ", textprops=dict(color="#3f7f93", size=15, rotation=90, ha='left', va='bottom'))
ybox = VPacker(children=[ybox1, ybox2], align="bottom", pad=0, sep=5)

anchored_ybox = AnchoredOffsetbox(loc=8, child=ybox, pad=0.1, frameon=False, bbox_to_anchor=(-0.06, 0.04),
                                  bbox_transform=ax.transAxes, borderpad=0.)

ax.add_artist(anchored_ybox)

plt.xticks(ind, peer_deal)
plt.yticks([])
plt.xlabel("Peer Deal in 2018")
plt.ylabel("% Districts")

for p in ax.patches:
    width, height = p.get_width(), p.get_height()
    x, y = p.get_xy()
    ax.annotate('{:.0f} %'.format(height * 100), (p.get_x() + .4 * width, p.get_y() + .45 * height), color='white', size=14)

# save image
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id5021_peer_deal_n100k_aff_2018sots.png')
