import os
import psycopg2
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.pyplot import gca
from matplotlib._png import read_png

# connection credentials
HOST = os.environ.get("HOST_DAR_PROD")
USER = os.environ.get("USER_DAR_PROD")
PASSWORD = os.environ.get("PASSWORD_DAR_PROD")
DB = os.environ.get("DB_DAR_PROD")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id1016_median_bw_per_student_all_and_not_meeting.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

data = pd.DataFrame(rows, columns=names)
myConnection.close()

med_bw = data

fig, ax = plt.subplots()

frame1 = plt.gca()

prop = fm.FontProperties()
prop.set_family('Lato')

sns.set_style('white', {'axes.linewidth': .5, 'axes.edgecolor':'#A1A1A1'})
sns.despine(left=True, right=True)

#median for districts meeting
plt.plot(med_bw.funding_year, med_bw.median_ia_bandwidth_per_student_kbps_meeting,
         color="#a74c00", lw = 1, zorder=2, label='Distrincts meeting 100kbps')

#unhighlighted markers for meeting group
plt.scatter(med_bw[med_bw.funding_year < 2019].funding_year,
            med_bw[med_bw.funding_year < 2019].median_ia_bandwidth_per_student_kbps_meeting,
            color="#cea68f", zorder=6, s=100, label='_nolegend_')


#label districts meeting unhighlighted markers
for i in range(med_bw.funding_year.min(),med_bw.funding_year.max()):
               ax.text(i-.01, med_bw[med_bw.funding_year == i].median_ia_bandwidth_per_student_kbps_meeting+6,
               int(round(med_bw[med_bw.funding_year == i].reset_index().median_ia_bandwidth_per_student_kbps_meeting[0])),
               color="#b6b6b6", fontproperties=prop,size=14, ha='right', zorder=9)

#districts meeting highlighted marker
plt.scatter(med_bw[med_bw.funding_year == 2019].funding_year,
            med_bw[med_bw.funding_year == 2019].median_ia_bandwidth_per_student_kbps_meeting,
            color="#bf6b3b", zorder=4, s=100, label='_nolegend_')

#label districts meeting highlighted marker
ax.text(2019-.01, med_bw[med_bw.funding_year == 2019].median_ia_bandwidth_per_student_kbps_meeting+6,
        int(round(med_bw[med_bw.funding_year == 2019].reset_index().median_ia_bandwidth_per_student_kbps_meeting[0])),
        color="#5e5d5d", fontproperties=prop,size=14, ha='right', zorder=9)


#all districts plot grey lines
plt.plot(med_bw.funding_year, med_bw.median_ia_bandwidth_per_student_kbps_all,
         color="#7E7D7D", lw = 1.5, zorder=1, label='All Districts')

#all districts unhighlighted markers
plt.scatter(med_bw[med_bw.funding_year < 2019].funding_year,
            med_bw[med_bw.funding_year < 2019].median_ia_bandwidth_per_student_kbps_all,
            color="#fac4a5", zorder=5, s=100, label='_nolegend_')

#label all districts unhighlighted markers
for i in range(med_bw.funding_year.min(),med_bw.funding_year.max()):
    ax.text(i-.01, med_bw[med_bw.funding_year == i].median_ia_bandwidth_per_student_kbps_all-37,
            int(round(med_bw[med_bw.funding_year == i].reset_index().median_ia_bandwidth_per_student_kbps_all[0])),
            color="#7E7D7D", fontproperties=prop,size=14, ha='left', zorder=9)

#all districts highlighted marker
plt.scatter(med_bw[med_bw.funding_year == 2019].funding_year, med_bw[med_bw.funding_year == 2019].median_ia_bandwidth_per_student_kbps_all,
            color="#f26c23", zorder=3, s=100, label='_nolegend_')

#label all districts highlighted marker
ax.text(2019-.01, med_bw[med_bw.funding_year == 2019].median_ia_bandwidth_per_student_kbps_all-40,
        int(round(med_bw[med_bw.funding_year == 2019].reset_index().median_ia_bandwidth_per_student_kbps_all[0])),
        color="#5e5d5d", fontproperties=prop,size=14, ha='left', zorder=9)


plt.legend(framealpha=1, frameon=True)

#x axis
plt.xticks(med_bw.funding_year)
for label in ax.get_xticklabels() :
    label.set_fontproperties(prop)
ax.tick_params(axis='x', colors='#A1A1A1')

#y axis
frame1.axes.get_yaxis().set_ticks([])
plt.ylabel('median bandwidth per student (kbps)', color="#A1A1A1", fontproperties=prop,size=14)

#title
plt.title("The median bandwidth/student continues \nto grow at a steady pace", fontproperties=prop,size=16)

plt.savefig(GITHUB+'/''figure_images/id1016_median_bw_per_student_all_and_not_meeting.png')
