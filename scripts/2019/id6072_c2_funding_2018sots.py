##imports and definitions
#import packages
import psycopg2
from pandas import DataFrame, concat, read_csv
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.pyplot import gca

#import environment variables
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")
HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
PORT_DAR = os.environ.get("PORT_DAR")
HOST_DENIM = os.environ.get("HOST_DENIM")
USER_DENIM = os.environ.get("USER_DENIM")
PASSWORD_DENIM = os.environ.get("PASSWORD_DENIM")
DB_DENIM = os.environ.get("DB_DENIM")
PORT_DENIM = os.environ.get("PORT_DENIM")

#query data
def getData( conn, filename ) :
    #source of sql files
    os.chdir(GITHUB+'/Projects/sots-isl/scripts/2019') 
    #query data
    cur = conn.cursor()
    cur.execute(open(filename, "r").read())
    names = [ x[0] for x in cur.description]
    rows = cur.fetchall()
    return DataFrame( rows, columns=names)


##connect to database and save query into pandas dataframe
#open connection to DAR
myConnection = psycopg2.connect( host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR, port=PORT_DAR)

#pull bandwidths from DB
spent = getData( myConnection, 'id4019_c2_funds_used_2018sots.sql')
remains = getData( myConnection, 'id4013_c2_funds_remaining_2018sots.sql')
at_risk_not_used = getData( myConnection, 'id4006_c2_funds_at_risk_for_0_year_districts_2018sots.sql')
at_risk_5_year_approaching = getData( myConnection, 'id4004_c2_funds_at_risk_expiring_in_2019_2018sots.sql')

#close connection to DAR
myConnection.close()

##properties for plotting template
threshold = 0
threshold_label= ''

label_title = 'There is E-rate funding at risk \nif districts dont act now.'
label_yaxis = ''


spent  = [0,round(float(spent.c2_funds_used[0]),-6)]

remainsNoRisk = [0,round(float(remains['sum'][0])-round(float(at_risk_not_used.c2_funds_at_risk_for_0_year_districts[0]),-6)-round(float(at_risk_5_year_approaching.c2_funds_at_risk_expiring_in_2019[0]),-6),-6)]

remainsRiskNotUsed = [0,round(float(at_risk_not_used.c2_funds_at_risk_for_0_year_districts[0]),-6)]

remainsRisk5YrApprchng =  [0,round(float(at_risk_5_year_approaching.c2_funds_at_risk_expiring_in_2019[0]),-6)]

spent_label = 'Spent'
remainsNoRisk_label = 'Remains, Future Risk'
remainsRiskNotUsed_label = 'Remains, 0-Yr'
remainsRisk5YrApprchng_label = 'Remains, 5-Yr'

##plotting template
fig, ax = plt.subplots()
frame1 = plt.gca()

LATO_REG = fm.FontProperties()
LATO_REG.set_family('Lato')
#for pc users -- dont merge into github with the filepath. merge in with the above code and ask a mac friend to test the code for you.
#LATO_REG = fm.FontProperties(fname='G:/My Drive/ESH Main Share/Marketing/Branding/Fonts and Colors/Fonts/lato/Lato-Regular.ttf')

#white background and no frame except bottom
sns.set_style('white', {'axes.linewidth': .5, 'axes.edgecolor':'#A1A1A1'})
sns.despine(left=True, right=True)

#plot grey lines between all data points
p1 = plt.bar([1,2], spent, .5, color='#d2e7b7')
p2 = plt.bar([1,2], remainsNoRisk, .5, color='#cccccc', bottom=spent)
p3 = plt.bar([1,2], remainsRiskNotUsed, .5, color='#68ab44', bottom=[sum(x) for x in zip(*[spent,remainsNoRisk])])
p4 = plt.bar([1,2], remainsRisk5YrApprchng, .5, color='#90c84d', bottom=[sum(x) for x in zip(*[spent,remainsNoRisk,remainsRiskNotUsed])])

#add value labels to each bar
ax.text(2,100000000, '$'+str(round(spent[1]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(2,spent[1]+100000000, '$'+str(round(remainsNoRisk[1]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(2,spent[1]+remainsNoRisk[1]+100000000, '$'+str(round(remainsRiskNotUsed[1]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(2,spent[1]+remainsNoRisk[1]+remainsRiskNotUsed[1]+100000000, '$'+str(round(remainsRisk5YrApprchng[1]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)

#add labels to each bar
ax.text(1.7,100000000, spent_label, color="#d2e7b7", fontproperties=LATO_REG,size=14, ha='right', zorder=9)
ax.text(1.7,spent[1]+100000000, remainsNoRisk_label, color="#cccccc", fontproperties=LATO_REG,size=14, ha='right', zorder=9)
ax.text(1.7,spent[1]+remainsNoRisk[1]+100000000, remainsRiskNotUsed_label, color="#68ab44", fontproperties=LATO_REG,size=14, ha='right', zorder=9)
ax.text(1.7,spent[1]+remainsNoRisk[1]+remainsRiskNotUsed[1]+100000000, remainsRisk5YrApprchng_label, color="#90c84d", fontproperties=LATO_REG,size=14, ha='right', zorder=9)

#format x axis font
plt.xticks([1,2], ['', ''])
for label in ax.get_xticklabels() :
    label.set_fontproperties(LATO_REG)
ax.tick_params(axis='x', colors='#A1A1A1')

#format y axis font
plt.yticks([1000000000,2000000000,3000000000,4000000000,5000000000,6000000000], ['$1.0', '$2.0', '$3.0', '$4.0', '$5.0', ''])
plt.ylabel(str(label_yaxis), color="#A1A1A1", fontproperties=LATO_REG,size=14)
for label in ax.get_yticklabels() :
    label.set_fontproperties(LATO_REG)
ax.tick_params(axis='y', colors='#A1A1A1')

#title
plt.title(str(label_title), fontproperties=LATO_REG,size=18)


#plt.show()
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id6072_c2_funding_2018sots.png')
