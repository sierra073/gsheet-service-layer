import psycopg2 as psy
import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

HOST = "charizard-psql1.cyttrh279zkr.us-east-1.rds.amazonaws.com"
USER = os.environ.get("USER_SPINER")
PASSWORD = os.environ.get("PASSWORD_SPINER")
DB = "sots_snapshot_2019_08_19"

GITHUB = os.environ.get("GITHUB")
DATA_PATH = GITHUB + '/Projects/peer_deal_methodology_updates_testing/data/'

def get_data(sql_file):
    queryfile = open(sql_file, 'r')
    query = queryfile.read()
    queryfile.close()
    success = None
    count = 0
    print ("  Trying to establish initial connection to the server...")
    while success is None:
        conn = psy.connect(host=HOST, user=USER, password=PASSWORD, database=DB)
        cur = conn.cursor()
        try:
            cur.execute(query)
            print("    ...Success!")
            success = "true"
            names = [x[0] for x in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=names)
            return df
        except psy.DatabaseError:
            print('  Server closed connection, trying again')
            if count == 10:
                print('  Please fix query logic')
                raise Exception('  Please fix query logic')
                print('******************************')
            else:
                count += 1
                pass

df_closest_district = get_data(GITHUB+'/Projects/sots-isl/scripts/2019/prework_queries/id7022_any_district_peer_sp_miles.sql')

df_closest_district['old_peersp_parent_name'] = df_closest_district['peersp_parent_name']
esh_650_not_matched = pd.read_csv(DATA_PATH + 'processed/esh_sps_not_80_geotel_sps.csv')

df_closest_district = df_closest_district[df_closest_district.old_peersp_parent_name.isin(esh_650_not_matched['esh_sp'])]

df_min  = df_closest_district.groupby(['district_id','district_state_code'],as_index=False)['distance_from_closet_district_w_provider'].min()
df_closest_district = pd.merge(df_min,df_closest_district, how = 'inner')

df = pd.DataFrame(df_closest_district.groupby(['already_receives_peer_deal_provider'],as_index=True)['district_id'].nunique())
df['percent'] = df['district_id']/df_closest_district.district_id.nunique()
df.rename(columns={'district_id':'districts'},inplace=True)
df.to_csv(GITHUB+'/Projects/sots-isl/data/id7023_missing_provider_already_received.csv',index=False)