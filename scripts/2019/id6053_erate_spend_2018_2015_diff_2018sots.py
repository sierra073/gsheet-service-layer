##imports and definitions
#import packages
import psycopg2
from pandas import DataFrame, concat

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


##connect to database and save query into pandas dataframe
#open connection to DAR
myConnection = psycopg2.connect( host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR, port=PORT_DAR)

#pull bandwidths from DB
df = getData( myConnection, 'historical_cost_mbps_2018sots.sql')

#close connection to DAR
myConnection.close()


##print diff
diff = float(df.overall_funding[df.funding_year == 2019].reset_index(drop=True)[0]) - float(df.overall_funding[df.funding_year == 2015].reset_index(drop=True)[0])
print(diff)