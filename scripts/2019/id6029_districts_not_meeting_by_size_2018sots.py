##imports and definitions
#import packages
import psycopg2
from pandas import DataFrame, concat
from numpy import zeros, arange, where
import seaborn as sns
import matplotlib.pyplot as plt
from pylab import gca
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
not_meeting = getData( myConnection, 'districts_not_meeting_2018sots.sql')
meeting_not_meeting = getData( myConnection, 'num_students_meeting_not_meeting_2018sots.sql')


#close connection to DAR
myConnection.close()


##plotting cost per student by category
fig, ax = plt.subplots()
sns.set(style="white")

plt.bar(range(not_meeting.num_students.count()),not_meeting.num_students, color = '#fdb913')

plt.axvline(x=not_meeting[not_meeting['num_students']>9000].num_students.count(), color="black", linestyle=':')

plt.ylim(0,80000)
plt.ylabel('number of students in the district')

val = round(round((meeting_not_meeting.num_districts_not_meeting_100_gt_9k_st[0]/meeting_not_meeting.num_districts_not_meeting_100[0]),2)*100,0)

plt.xlim(0,not_meeting.num_students.count()*1.3)
plt.xticks([0,not_meeting[not_meeting['num_students']>9000].num_students.count(), not_meeting.num_students.count()], ['0%',str(val)+'%', '100%'])
plt.yticks(range(0,100000,20000), ['0', '20k', '40k', '60k', '80k'])
plt.xlabel('% of districts')

ax.text(not_meeting[not_meeting['num_students']>9000].num_students.count()-1, 75000, 'large districts', color="black", fontweight='bold', ha='right', va='top', fontsize=10)
ax.text(not_meeting[not_meeting['num_students']>9000].num_students.count()-1, 70000, str(round(not_meeting[not_meeting['num_students']>9000].num_students.sum(),-5)/1000000)+'M students', color="black", ha='right', va='top', fontsize=10)
ax.text(not_meeting[not_meeting['num_students']>9000].num_students.count()-1, 65000, 'only '+str(not_meeting[not_meeting['num_students']>9000].num_students.count())+' districts', color="black", ha='right', va='top', fontsize=10)

#plt.show()
fig.savefig(GITHUB+'/''figure_images/id6029_districts_not_meeting_by_size_2018sots.png')

