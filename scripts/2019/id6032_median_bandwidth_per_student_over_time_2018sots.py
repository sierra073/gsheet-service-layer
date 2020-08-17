##imports and definitions
#import packages
import psycopg2
from pandas import DataFrame, concat
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.lines import Line2D
import matplotlib.font_manager as fm
from matplotlib.pyplot import gca

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


# connect to DAR and save list of FRNs into pandas dataframe
# open connection to DAR
myConnection = psycopg2.connect(host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR, port=PORT_DAR)

# pull bandwidths from DB
bandwidth_over_time = getData(myConnection, 'median_bandwidth_per_student_over_time_2018sots.sql')
bandwidth_over_time = bandwidth_over_time[bandwidth_over_time.funding_year != 2013]
# close connection to DAR
myConnection.close()

#remove 2013
bandwidth_over_time = bandwidth_over_time[bandwidth_over_time.funding_year > 2014]
##plotting median bandwidth over time for 
fig, ax = plt.subplots()

frame1 = plt.gca()
#prop = fm.FontProperties(fname='G:/My Drive/ESH Main Share/Marketing/Branding/Fonts and Colors/Fonts/lato/Lato-Regular.ttf')
prop = fm.FontProperties()
prop.set_family('Lato')


#white background and no frame except bottom
sns.set_style('white', {'axes.linewidth': .5, 'axes.edgecolor':'#A1A1A1'})
sns.despine(left=True, right=True)

#districts not meeting plot grey lines
plt.plot(bandwidth_over_time.funding_year, bandwidth_over_time.median_ia_bandwidth_per_student_kbps_not_meeting, color="#636363", lw = 1, zorder=2)

#districts not meeting unhighlighted markers
plt.scatter(bandwidth_over_time[bandwidth_over_time.funding_year < 2019].funding_year, 
bandwidth_over_time[bandwidth_over_time.funding_year < 2019].median_ia_bandwidth_per_student_kbps_not_meeting, color="#fac4a5", zorder=6, s=100)

#label districts not meeting unhighlighted markers
for i in range(bandwidth_over_time.funding_year.min(),bandwidth_over_time.funding_year.max()):
    ax.text(i-.05, bandwidth_over_time[bandwidth_over_time.funding_year == i].median_ia_bandwidth_per_student_kbps_not_meeting+5, str(round(bandwidth_over_time[bandwidth_over_time.funding_year == i].astype(float).reset_index().median_ia_bandwidth_per_student_kbps_not_meeting[0],1))+' kbps', color="#fac4a5", fontproperties=prop,size=14, ha='left', zorder=9)

#districts meeting highlighted marker
plt.scatter(bandwidth_over_time[bandwidth_over_time.funding_year == 2019].funding_year, bandwidth_over_time[bandwidth_over_time.funding_year == 2019].median_ia_bandwidth_per_student_kbps_not_meeting, color="#f26c23", zorder=4, s=100)

#label districts not meeting highlighted marker
ax.text(2019-.05, bandwidth_over_time[bandwidth_over_time.funding_year == 2019].median_ia_bandwidth_per_student_kbps_not_meeting+5, str(round(bandwidth_over_time[bandwidth_over_time.funding_year == 2019].astype(float).reset_index().median_ia_bandwidth_per_student_kbps_not_meeting[0],1))+' kbps', color="#f26c23", fontproperties=prop,size=14, ha='left', zorder=9)


#districts meeting plot grey lines
plt.plot(bandwidth_over_time.funding_year, bandwidth_over_time.median_ia_bandwidth_per_student_kbps_meeting, color="#636363", lw = 1.5, zorder=1)

#districts meeting unhighlighted markers
plt.scatter(bandwidth_over_time[bandwidth_over_time.funding_year < 2019].funding_year, 
bandwidth_over_time[bandwidth_over_time.funding_year < 2019].median_ia_bandwidth_per_student_kbps_meeting, color="#fac4a5", zorder=5, s=200)

#label districts meeting unhighlighted markers
for i in range(bandwidth_over_time.funding_year.min(),bandwidth_over_time.funding_year.max()):
    ax.text(i-.05, bandwidth_over_time[bandwidth_over_time.funding_year == i].median_ia_bandwidth_per_student_kbps_meeting-30, str(round(bandwidth_over_time[bandwidth_over_time.funding_year == i].astype(float).reset_index().median_ia_bandwidth_per_student_kbps_meeting[0],1))+' kbps', color="#fac4a5", fontproperties=prop,size=14, ha='left', zorder=9)

#districts meeting highlighted marker
plt.scatter(bandwidth_over_time[bandwidth_over_time.funding_year == 2019].funding_year, bandwidth_over_time[bandwidth_over_time.funding_year == 2019].median_ia_bandwidth_per_student_kbps_meeting, color="#f26c23", zorder=3, s=200)

#label districts meeting highlighted marker
ax.text(2019-.05, bandwidth_over_time[bandwidth_over_time.funding_year == 2019].median_ia_bandwidth_per_student_kbps_meeting-30, str(round(bandwidth_over_time[bandwidth_over_time.funding_year == 2019].astype(float).reset_index().median_ia_bandwidth_per_student_kbps_meeting[0],1))+' kbps', color="#f26c23", fontproperties=prop,size=14, ha='left', zorder=9)


#100kbps context line
plt.axhline(y=100, color="#A1A1A1", linestyle='--')

#label 100kbps line
ax.text(bandwidth_over_time.funding_year.min()+.5, 105, '100 kbps', color="#A1A1A1", fontproperties=prop,size=14, ha='right')


#x axis
plt.xticks(bandwidth_over_time.funding_year)
for label in ax.get_xticklabels() :
    label.set_fontproperties(prop)
ax.tick_params(axis='x', colors='#A1A1A1')

#y axis
frame1.axes.get_yaxis().set_ticks([])
plt.ylabel('median bandwidth per student\n by goal meeting status', color="#A1A1A1", fontproperties=prop,size=14)

#title
plt.title("The median bandwidth/student continues \nto grow for districts without constraints.", fontproperties=prop,size=18)

# plt.show()
fig.savefig(GITHUB + '/Projects/sots-isl/figure_images/id6032_median_bandwidth_per_student_over_time_2018sots.png')
