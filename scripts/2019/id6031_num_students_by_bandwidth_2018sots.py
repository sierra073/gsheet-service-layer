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
districts = getData( myConnection, 'districts_fit_for_ia_2018sots.sql')
meeting_not_meeting = getData( myConnection, 'num_students_meeting_not_meeting_2018sots.sql')


#close connection to DAR
myConnection.close()


##plotting cost per student by category
fig, ax = plt.subplots()
sns.set(style="white")

bi=3000
axi=3000
r100=int(100/districts.ia_bandwidth_per_student_kbps_adj.max()*bi)
r1000=int(1000/districts.ia_bandwidth_per_student_kbps_adj.max()*bi)
rax=int(axi/districts.ia_bandwidth_per_student_kbps_adj.max()*bi)

n, bins, patches = plt.hist(districts.ia_bandwidth_per_student_kbps_adj.astype(float) , bins = bi, weights=districts.num_students, color = '#fbdbca')
for i in range(r100):
    patches[i].set_fc('#f26c23')
for i in range(r1000,axi):
    patches[i].set_fc('#f26c23')


plt.axvline(x=100, color="black", linestyle=':')
plt.axvline(x=1000, color="black", linestyle=':')

ax.text(100,800000, str(meeting_not_meeting.num_students_not_meeting_100[0]/1000000)+'M students not meeting 100 kbps', color="#f26c23", fontweight='bold')
ax.text(1000,300000, str(meeting_not_meeting.num_students_meeting_1m[0]/1000000)+'M students meeting 1 Mbps', color="#f26c23", fontweight='bold')


plt.xlim(0,axi)
plt.xticks([100,1000], ['100 kbps', '1 Mbps'])
plt.xlabel('kbps/student')

plt.yticks([], '')
plt.ylabel('number of students')

#plt.show()
fig.savefig(GITHUB+'/''figure_images/id6031_num_students_by_bandwidth_2018sots.png')



