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

#import environment variables
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
PORT_DAR = os.environ.get("PORT_DAR")
GITHUB = os.environ.get("GITHUB")

#query data
def getData( conn, filename ) :
    #source of sql files
    os.chdir(GITHUB+'/''scripts/2019/prework_queries') 
    #query data
    cur = conn.cursor()
    cur.execute(open(filename, "r").read())
    names = [ x[0] for x in cur.description]
    rows = cur.fetchall()
    return DataFrame( rows, columns=names)

#decimal range
def frange(x, y, jump):
  while x < y:
    yield x
    x += jump


##connect to DAR and save list of FRNs into pandas dataframe
#open connection to DAR
myConnection = psycopg2.connect( host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR, port=PORT_DAR)

#pull bandwidths from DB
newly_meeting = getData( myConnection, 'bw_cost_change_for_newly_meeting_w_deal_2018sots.sql')


#close connection to DAR
myConnection.close()


##plotting cost per student by category
fig, ax = plt.subplots()
sns.set(style="white")

plt.subplot(212)
plt.plot(range(2),[newly_meeting.median_ia_bandwidth_per_student_kbps_17[0],newly_meeting.median_ia_bandwidth_per_student_kbps_18[0]], '-o', color = '#fdb913')
plt.axhline(y=100, color="black", linestyle=':')
plt.ylabel('median internet access\nkbps/student ')
plt.xticks(range(3), [2018,2019,''])
plt.yticks([100,300])
plt.ylim(0,1000)

ax1=plt.subplot(212)
ax1.text(.3, 500, str(round(newly_meeting.median_ia_bandwidth_per_student_kbps_18[0]/newly_meeting.median_ia_bandwidth_per_student_kbps_17[0],1))+'x bandwidth', color="black")


plt.subplot(211)
plt.plot(range(2),[newly_meeting.median_ia_monthly_cost_total_17[0],newly_meeting.median_ia_monthly_cost_total_18[0]], '-o', color = '#fdb913')
plt.ylabel('median internet access\nmonthly cost')
plt.yticks([2000,3000],['$2k','$3k'])
plt.xticks(range(3), ['','',''])

ax2=plt.subplot(211)
ax2.text(.3, 2500, '$'+str(-round(newly_meeting.median_ia_monthly_cost_total_change[0],0))+' less each month', color="black")


#plt.show()
fig.savefig(GITHUB+'/''figure_images/id6030_bw_cost_change_for_newly_meeting_w_deal_2018sots.png')

