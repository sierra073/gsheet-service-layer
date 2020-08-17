import os
import psycopg2
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.pyplot import gca

# connection credentials
HOST = os.environ.get("HOST_DAR_PROD")
USER = os.environ.get("USER_DAR_PROD")
PASSWORD = os.environ.get("PASSWORD_DAR_PROD")
DB = os.environ.get("DB_DAR_PROD")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER,
                                password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
queryfile = open('id101415_num_students_meeting_1mbps.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()

data = pd.DataFrame(rows, columns=names)
myConnection.close()

students_1mbps = data

fig, ax = plt.subplots()
frame1 = plt.gca()

prop = fm.FontProperties()
prop_i = fm.FontProperties()
prop.set_family('Lato')
prop_i.set_family('Lato')
prop_i.set_style('italic')


#background
sns.set_style('white', {'axes.linewidth': .5, 'axes.edgecolor':'#A1A1A1'})
sns.despine(left=True, right=True)

#unhighlighted marker
plt.scatter(students_1mbps[students_1mbps.funding_year < 2019].funding_year,
            students_1mbps[students_1mbps.funding_year < 2019].num_students_meeting_ext,
            color="#fac4a5", zorder=6, s=100)

# label for unhighlighted marker
for i in range(students_1mbps.funding_year.min(),students_1mbps.funding_year.max()):
    ax.text(i-.05, students_1mbps[students_1mbps.funding_year == i].num_students_meeting_ext+1,
            str(round(students_1mbps[students_1mbps.funding_year == i].reset_index().num_students_meeting_ext[0],1))+'M',
            color="#fac4a5", fontproperties=prop,size=14, ha='right')

#highlighted marker
plt.scatter(students_1mbps[students_1mbps.funding_year == 2019].funding_year,
            students_1mbps[students_1mbps.funding_year == 2019].num_students_meeting_ext,
            color="#f26c23", zorder=2, s=200)

# label for highlighted marker
ax.text(2019-.09, students_1mbps[students_1mbps.funding_year == 2019].num_students_meeting_ext+1,
        str(round(students_1mbps[students_1mbps.funding_year == 2019].reset_index().num_students_meeting_ext[0],1))+'M',
        color="#f26c23", fontproperties=prop,size=14, ha='right')

#adding line
plt.plot(students_1mbps.funding_year, students_1mbps.num_students_meeting_ext, color="#636363", zorder=1)

#x axis
xaxis = list(range(students_1mbps.funding_year.min()-1,students_1mbps.funding_year.max()+1))
xaxis[xaxis == 2014] = ''
plt.xticks(list(range(students_1mbps.funding_year.min()-1,students_1mbps.funding_year.max()+1)), xaxis)
for label in ax.get_xticklabels() :
    label.set_fontproperties(prop)
ax.tick_params(axis='x', colors='#010101')

#y axis
frame1.axes.get_yaxis().set_ticks([])
plt.ylabel('# of students meeting 1mbps in millions', color="#010101", fontproperties=prop,size=14)

fig.savefig(GITHUB+'/Projects/sots-isl/figure_images/id1014_number_students_meeting_1mbps_yoy.png')
