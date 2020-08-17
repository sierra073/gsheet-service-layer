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
HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
PORT_DAR = os.environ.get("PORT_DAR")
GITHUB = os.environ.get("GITHUB")

#import data from previous script
os.chdir(GITHUB+'/Projects/sots-isl/data/') 
fund_1m = read_csv('id2035_regroup_meet1mbps.csv')

#query data
def getData( conn, filename ) :
    #source of sql files
    os.chdir(GITHUB+'/Projects/sots-isl/scripts/2019/prework_queries') 
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
df = getData( myConnection, 'historical_cost_mbps_2018sots.sql')

#close connection to DAR
myConnection.close()

##properties for plotting template
threshold = 3900000000
threshold_label= '$3.9 B E-rate cap'

label_title = 'There is E-rate funding available \nto support 1 Mbps for every student.'
label_yaxis = ''

bottomBar = [0,fund_1m.extrap_extra_erate_funding[fund_1m.district_regroup == "Pay More to Meet"].reset_index(drop=True)[0].round(-6)]
bottomBar_label = 'Additional funding for 1Mbps\n to Traditional Schools'
topBar =  [0,float(df.overall_funding[df.funding_year == 2019].reset_index(drop=True)[0])]
topBar_label = '2019 E-rate funding'

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
p1 = plt.bar([1,2], bottomBar, 1, color='#f26c23')
p2 = plt.bar([1,2], topBar, 1, color='#fac4a5', bottom=bottomBar)

#add value labels to each bar
ax.text(2,bottomBar[1]/2, '$'+str(round(bottomBar[1]/1000000000,1))+' B', color="black", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(2,(bottomBar[1]+topBar[1])/2, '$'+str(round(topBar[1]/1000000000,1))+' B', color="black", fontproperties=LATO_REG,size=10, ha='left', zorder=9)

#add labels to each bar
ax.text(1.4,bottomBar[1]/2, bottomBar_label, color="#f26c23", fontproperties=LATO_REG,size=14, ha='right', zorder=9)
ax.text(1.4,(bottomBar[1]+topBar[1])/2, topBar_label, color="#fac4a5", fontproperties=LATO_REG,size=14, ha='right', zorder=9)

#plot vertical threshold line
plt.axhline(y=threshold, color="#A1A1A1", linestyle='--')
#label vertical threshold line
ax.text(1.9, threshold+50000000, threshold_label, color="#A1A1A1", fontproperties=LATO_REG, size=14, ha='left')

#format x axis font
plt.tick_params(
    axis='x',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    bottom=False,      # ticks along the bottom edge are off
    top=False,         # ticks along the top edge are off
    labelbottom=False) # labels along the bottom edge are off
    
#format y axis font
plt.yticks([1000000000,2000000000,3000000000,4000000000,5000000000], ['$1.0', '$2.0', '$3.0', '$4.0', ''])
plt.ylabel(str(label_yaxis), color="#A1A1A1", fontproperties=LATO_REG,size=14)
for label in ax.get_yticklabels() :
    label.set_fontproperties(LATO_REG)
ax.tick_params(axis='y', colors='#A1A1A1')

#title
plt.title(str(label_title), fontproperties=LATO_REG,size=18)


#plt.show()
os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
plt.savefig('id6052_erate_spend_vs_1m_fund_2018sots.png')
