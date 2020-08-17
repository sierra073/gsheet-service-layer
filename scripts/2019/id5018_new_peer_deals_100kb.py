##imports and definitions
import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats

# HOST = os.environ.get("HOST_SPINER")
# USER = os.environ.get("USER_SPINER")
# PASSWORD = os.environ.get("PASSWORD_SPINER")
USER = "eshadmin"
PASSWORD = "J8IkWgrwsxC&"
HOST = "esh-psql1.educationsuperhighway.org"
DB = 'molly_100kpeers_update_copyfrzn'
GITHUB = os.environ.get("GITHUB")


def main():
    # connect to dar and save list of all clean for cost districts (excl. AK) with all of the relevant metrics/indicators
    print(HOST, USER,PASSWORD,DB )
    myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

    cur = myConnection.cursor()

    os.chdir(GITHUB + '/''scripts/2019/prework_queries')
    queryfile = open('id5018_new_peer_deals_100kb.sql', 'r')
    query = queryfile.read()
    queryfile.close()

    success = None
    print ("Trying to establish initial connection to the server")
    while success is None:
        conn = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DB)
        cur = conn.cursor()
        try:
            cur.execute(query)
            print("Success!")
            success = "true"
            break
        except psycopg2.DatabaseError:
            print('Server closed connection, trying again')
            pass
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=names)

    myConnection.close()

    df.to_csv(GITHUB+'/''data/'+os.path.basename(__file__).replace('.py','.csv'),index=False)


if __name__ == '__main__':
    main()
