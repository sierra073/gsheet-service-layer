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
    queryfile = open(filename, 'r')
    query = queryfile.read()
    queryfile.close()

    cur.execute(query)
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

# multiple of cost per student by category
print(meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Meeting 100kbps Goal'].median_ia_annual_cost_per_student.reset_index(drop=True)[0] / meeting_not_meeting[meeting_not_meeting['subgroup'] == 'Insufficient Budget'].median_ia_annual_cost_per_student.reset_index(drop=True)[0])
