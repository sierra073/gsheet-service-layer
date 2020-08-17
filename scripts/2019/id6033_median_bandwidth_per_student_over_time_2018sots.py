##imports and definitions
#import packages
import psycopg2
from pandas import DataFrame, concat
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import matplotlib.font_manager as fm
from matplotlib.pyplot import gca
from matplotlib.cbook import get_sample_data
from matplotlib.offsetbox import (TextArea, DrawingArea, OffsetImage,
                                  AnnotationBbox)
from matplotlib._png import read_png

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


##connect to DAR and save list of FRNs into pandas dataframe
#open connection to DAR
myConnection = psycopg2.connect( host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR, port=PORT_DAR)

#pull bandwidths from DB
bandwidth_over_time = getData( myConnection, 'median_bandwidth_per_student_over_time_2018sots.sql')


#close connection to DAR
myConnection.close()

##plotting median bandwidth over time for 
fig, ax = plt.subplots()
frame1 = plt.gca()

#prop = fm.FontProperties(fname='G:/My Drive/ESH Main Share/Marketing/Branding/Fonts and Colors/Fonts/lato/Lato-Regular.ttf')
#prop_i = fm.FontProperties(fname='G:/My Drive/ESH Main Share/Marketing/Branding/Fonts and Colors/Fonts/lato/Lato-Italic.ttf')

prop = fm.FontProperties()
prop_i = fm.FontProperties()
prop.set_family('Lato')
prop_i.set_family('Lato')
prop_i.set_style('italic')


#white background and no frame except bottom
sns.set_style('white', {'axes.linewidth': .5, 'axes.edgecolor':'#A1A1A1'})
sns.despine(left=True, right=True)

#unhighlighted markers
plt.scatter(bandwidth_over_time[bandwidth_over_time.funding_year < 2019].funding_year, 
bandwidth_over_time[bandwidth_over_time.funding_year < 2019].median_ia_bandwidth_per_student_kbps, color="#fac4a5", zorder=3, s=200)

#no data markers
plt.scatter(2014, 
(bandwidth_over_time[bandwidth_over_time.funding_year == 2013].reset_index().median_ia_bandwidth_per_student_kbps[0]+bandwidth_over_time[bandwidth_over_time.funding_year == 2015].reset_index().median_ia_bandwidth_per_student_kbps[0])/2, color="#fac4a5", zorder=3, s=200)
plt.scatter(2014, 
(bandwidth_over_time[bandwidth_over_time.funding_year == 2013].reset_index().median_ia_bandwidth_per_student_kbps[0]+bandwidth_over_time[bandwidth_over_time.funding_year == 2015].reset_index().median_ia_bandwidth_per_student_kbps[0])/2, color="white", zorder=3, s=100)

#label no data
ax.text(2014-.2, (bandwidth_over_time[bandwidth_over_time.funding_year == 2013].reset_index().median_ia_bandwidth_per_student_kbps[0]+bandwidth_over_time[bandwidth_over_time.funding_year == 2015].reset_index().median_ia_bandwidth_per_student_kbps[0])/2, 'No Data', color="#A1A1A1", fontproperties=prop_i,size=14, ha='right')


#highlighted marker
plt.scatter(bandwidth_over_time[bandwidth_over_time.funding_year == 2019].funding_year, bandwidth_over_time[bandwidth_over_time.funding_year == 2019].median_ia_bandwidth_per_student_kbps, color="#f26c23", zorder=2, s=200)

#grey lines
plt.plot(bandwidth_over_time.funding_year, bandwidth_over_time.median_ia_bandwidth_per_student_kbps, color="#636363", zorder=1)

#100kbps context line
plt.axhline(y=100, color="#A1A1A1", linestyle='--')

#label 100kbps line
ax.text(bandwidth_over_time.funding_year.max()+.25, 105, '100 kbps', color="#A1A1A1", fontproperties=prop,size=14, ha='right')


#x axis
xaxis = list(range(bandwidth_over_time.funding_year.min()-1,bandwidth_over_time.funding_year.max()+1))
xaxis[xaxis == 2012] = ''
plt.xticks(list(range(bandwidth_over_time.funding_year.min()-1,bandwidth_over_time.funding_year.max()+1)), xaxis)
for label in ax.get_xticklabels() :
    label.set_fontproperties(prop)
ax.tick_params(axis='x', colors='#A1A1A1')

#y axis
frame1.axes.get_yaxis().set_ticks([])
plt.ylabel('median bandwidth per student', color="#A1A1A1", fontproperties=prop,size=14)

#image
# arr_hand = read_png(GITHUB+'/''figure_images/callout_lt_org.png')
# imagebox = OffsetImage(arr_hand, zoom=.5)
# iter = list(range(bandwidth_over_time.funding_year.min(),bandwidth_over_time.funding_year.max()+1))
# iter.remove(2014)
# for i in iter:
#     xy = [i-.5, bandwidth_over_time[bandwidth_over_time.funding_year == i].median_ia_bandwidth_per_student_kbps+101]
#     ab = AnnotationBbox(imagebox, xy,
#         xybox=(30., -30.),
#         xycoords='data',
#         boxcoords="offset points",
#         frameon=False)                                  
#     ax.add_artist(ab)

#plt.show()
fig.savefig(GITHUB+'/''figure_images/id6033_median_bandwidth_per_student_over_time_2018sots.png')
