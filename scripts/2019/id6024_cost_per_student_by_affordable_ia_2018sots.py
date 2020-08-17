##imports and definitions
#import packages
import psycopg2
from pandas import DataFrame, concat
from numpy import zeros, arange, where
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import matplotlib.ticker as mtick
from textwrap import wrap
from matplotlib import colors

# import environment variables
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
PORT_DAR = os.environ.get("PORT_DAR")
GITHUB = os.environ.get("GITHUB")

# query data


def getData(conn, filename):
    # source of sql files
    os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
    # query data
    cur = conn.cursor()
    cur.execute(open(filename, "r").read())
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    return DataFrame(rows, columns=names)

# decimal range


def frange(x, y, jump):
    while x < y:
        yield x
        x += jump


# connect to DAR and save list of FRNs into pandas dataframe
# open connection to DAR
myConnection = psycopg2.connect(host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR, port=PORT_DAR)

# pull bandwidths from DB
meeting_not_meeting = getData(myConnection, 'median_ia_annual_cost_per_student_2018sots.sql')


# close connection to DAR
myConnection.close()


# subset
not_meeting = meeting_not_meeting[~meeting_not_meeting['subgroup'].str.contains('Goal')]

# plotting cost per student by category
fig, ax = plt.subplots()
sns.set(style="white")

plt.barh(range(2), [meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Insufficient Budget'].median_ia_annual_cost_per_student.reset_index(drop=True)[0], meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Insufficient Budget'].median_ia_annual_cost_per_student.reset_index(drop=True)[0]], color=["#cb2128", "#cccccc"])
ax.text(float(meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Insufficient Budget'].median_ia_annual_cost_per_student.reset_index(drop=True)[0]) + .5, 0, '$' + str(meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Insufficient Budget'].median_ia_annual_cost_per_student.reset_index(drop=True)[0]), color="#cb2128", fontweight='bold')
ax.text(float(meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Sufficient Budget'].median_ia_annual_cost_per_student.reset_index(drop=True)[0]) + .5, 1, '$' + str(meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Sufficient Budget'].median_ia_annual_cost_per_student.reset_index(drop=True)[0]), color="#cccccc", fontweight='bold')


plt.axvline(x=meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Meeting 100kbps Goal'].median_ia_annual_cost_per_student.reset_index(drop=True)[0], color="black", linestyle=':')
ax.text(meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Meeting 100kbps Goal'].median_ia_annual_cost_per_student.reset_index(drop=True)[0] - 7, .6, 'Meeting 100kbps Goal', color="black")
ax.text(meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Meeting 100kbps Goal'].median_ia_annual_cost_per_student.reset_index(drop=True)[0] - 3, .5, '$' + str(meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Meeting 100kbps Goal'].median_ia_annual_cost_per_student.reset_index(drop=True)[0]), color="black")


plt.yticks(range(2), ['\n'.join(wrap(l, 19)) for l in not_meeting.subgroup.tolist()])
plt.ylabel('')


plt.xlabel('median annual cost per student')
ax.xaxis.set_major_formatter(mtick.StrMethodFormatter('${x:0.2f}'))


plt.subplots_adjust(left=.21)

# plt.show()
fig.savefig(GITHUB + '/Projects/sots-isl/figure_images/id6024_cost_per_student_by_affordable_ia_2018sots.png')
