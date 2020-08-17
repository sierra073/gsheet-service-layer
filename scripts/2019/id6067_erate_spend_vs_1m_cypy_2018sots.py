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

#import data from previous script
os.chdir(GITHUB+'/Projects/sots-isl/data/') 
fund_1m_2019 = read_csv('id2035_regroup_meet1mbps.csv')

#query data
def getData( conn, filename ) :
    #source of sql files
    os.chdir(GITHUB+'/Projects/sots-isl/scripts/2019/prework_queries/') 
    #query data
    cur = conn.cursor()
    cur.execute(open(filename, "r").read())
    names = [ x[0] for x in cur.description]
    rows = cur.fetchall()
    return DataFrame( rows, columns=names)

#query data
def getDataScript( conn, filename ) :
    #source of sql files
    os.chdir(GITHUB+'/Projects/sots-isl/scripts/') 
    #query data
    cur = conn.cursor()
    cur.execute(open(filename, "r").read())
    names = [ x[0] for x in cur.description]
    rows = cur.fetchall()
    return DataFrame( rows, columns=names)


def getData2018( conn, filename ) :
    #source of sql files
    os.chdir(GITHUB+'/Projects_SotS_2018/e-rate/') 
    #query data
    cur = conn.cursor()
    cur.execute(open(filename, "r").read())
    names = [ x[0] for x in cur.description]
    rows = cur.fetchall()
    return DataFrame( rows, columns=names)


##connect to database and save query into pandas dataframe
#open connection to DAR
myConnection = psycopg2.connect( host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR, port=PORT_DAR)

#pull from DB
df = getData( myConnection, 'historical_cost_mbps_2018sots.sql')

wan_2019 = getDataScript( myConnection, 'id6069_wan_projection_2018sots.sql')

other_2019 = getDataScript( myConnection, 'id6068_erate_spend_2018_2018_2018sots.sql')


#close connection to DAR
myConnection.close()

##connect to database and save query into pandas dataframe
#open connection to DENIM
myConnection = psycopg2.connect( host=HOST_DENIM, user=USER_DENIM, password=PASSWORD_DENIM, database=DB_DENIM, port=PORT_DENIM)

#pull from DB
other_2018 = getData( myConnection, 'erate_spend_2018_2018sots.sql')

internet_2018 = getData2018( myConnection, 'src/erate_cost_ak_increase_2018sots.sql')

wan_2018 = getData2018( myConnection, 'AK_increase/src/total_wan_costs_2018sots.sql')

c2_2018 = getData2018( myConnection, 'AK_increase/src/c2_spend_current_2018sots.sql')

fund_bb_2018 = getData2018( myConnection, 'src/erate_cost_average_ak_update_2018sots.sql')


#close connection to DENIM
myConnection.close()


##properties for plotting template
c2 = 1000000000

threshold = 4060000000
threshold_label= '$4.06 B E-rate cap'

label_title = 'The E-rate program now has sufficient resources to \nmeet the 1 Mbps per student goal.'
label_yaxis = ''


projInternet = [0,round(float(fund_bb_2017.total_cost[fund_bb_2017.category == 'Total IA Cost'].reset_index(drop=True)[0]),-9),fund_1m_2018.extrap_extra_erate_funding[fund_1m_2018.district_regroup == "Pay More to Meet"].reset_index(drop=True)[0].round(-6)+float(df.ia_funding[df.funding_year == 2019].reset_index(drop=True)[0])]

projWAN = [0,round(float(fund_bb_2017.total_cost[fund_bb_2017.category == 'Total WAN Cost'].reset_index(drop=True)[0]),-6),round(float(wan_2018.wan_funding[wan_2018.funding_year == 2019].reset_index(drop=True)[0]),-6)]

projC2 = [0,c2,c2]

projOther = [0,round(float(other_2018['sum'][0]),-6)-round(float(c2_2018['erate_wifi_17'][0]),-6)-round(float(wan_2017.extrap_wan_cost.sum()),-6)-round(float(internet_2017.total_cost[internet_2017.category.str.contains('Current', regex=False)].sum()),-6),round(float(other_2018.commitment_amount_request[(other_2018.funding_year == 2019) & (other_2018.categoy == 'Other')].sum()),-6)]

projInternet_label = 'Internet'
projWAN_label = 'WAN'
projC2_label = 'C2'
projOther_label = 'Other'

##plotting template
fig, ax = plt.subplots()
frame1 = plt.gca()

#LATO_REG = fm.FontProperties()
#LATO_REG.set_family('Lato')
#for pc users -- dont merge into github with the filepath. merge in with the above code and ask a mac friend to test the code for you.
LATO_REG = fm.FontProperties(fname='G:/My Drive/ESH Main Share/Marketing/Branding/Fonts and Colors/Fonts/lato/Lato-Regular.ttf')

#white background and no frame except bottom
sns.set_style('white', {'axes.linewidth': .5, 'axes.edgecolor':'#A1A1A1'})
sns.despine(left=True, right=True)

#plot grey lines between all data points
p1 = plt.bar([1,2,3], projInternet, .5, color='#90c84d')
p2 = plt.bar([1,2,3], projWAN, .5, color='#d2e7b7', bottom=projInternet)
p3 = plt.bar([1,2,3], projC2, .5, color='#68ab44', bottom=[sum(x) for x in zip(*[projInternet,projWAN])])
p4 = plt.bar([1,2,3], projOther, .5, color='#cccccc', bottom=[sum(x) for x in zip(*[projInternet,projWAN,projC2])])

#add value labels to each bar - 2018
ax.text(2,projInternet[1]/2, '$'+str(round(projInternet[1]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(2,(projInternet[1]+projWAN[1])/2, '$'+str(round(projWAN[1]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(2,(projInternet[1]+projWAN[1]+projC2[1])*3/4, '$'+str(round(projC2[1]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(2,(projInternet[1]+projWAN[1]+projC2[1]+projOther[1])*4/5, '$'+str(round(projOther[1]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)

#add value labels to each bar - 2019
ax.text(3,projInternet[2]/2, '$'+str(round(projInternet[2]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(3,(projInternet[2]+projWAN[2])/2, '$'+str(round(projWAN[2]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(3,(projInternet[2]+projWAN[2]+projC2[2])*3/4, '$'+str(round(projC2[2]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)
ax.text(3,(projInternet[2]+projWAN[2]+projC2[2]+projOther[2])*4/5, '$'+str(round(projOther[2]/1000000000,1))+' B', color="white", fontproperties=LATO_REG,size=10, ha='left', zorder=9)

#add labels to each bar
ax.text(1.7,100000000, projInternet_label, color="#90c84d", fontproperties=LATO_REG,size=14, ha='right', zorder=9)
ax.text(1.7,projInternet[1]+100000000, projWAN_label, color="#d2e7b7", fontproperties=LATO_REG,size=14, ha='right', zorder=9)
ax.text(1.7,projInternet[1]+projWAN[1]+100000000, projC2_label, color="#68ab44", fontproperties=LATO_REG,size=14, ha='right', zorder=9)
ax.text(1.7,projInternet[1]+projWAN[1]+projC2[1]+100000000, projOther_label, color="#cccccc", fontproperties=LATO_REG,size=14, ha='right', zorder=9)

#plot vertical threshold line
plt.axhline(y=threshold, color="#A1A1A1", linestyle='--')
#label vertical threshold line
ax.text(2.6, threshold+50000000, threshold_label, color="#A1A1A1", fontproperties=LATO_REG, size=14, ha='left')

#format x axis font
plt.xticks([1,2,3], ['', '2018', '2019'])
for label in ax.get_xticklabels() :
    label.set_fontproperties(LATO_REG)
ax.tick_params(axis='x', colors='#A1A1A1')
    
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
plt.savefig('id6067_erate_spend_vs_1m_fund_cypy.png')
