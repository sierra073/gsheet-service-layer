import os
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import sys

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")

sys.path.insert(0, GITHUB + "/''scripts/2019/prework_queries")
from id7001_model_functions import *

HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")

def get_data(sql_file):
    queryfile = open(sql_file, 'r')
    query = queryfile.read()
    queryfile.close()
    success = None
    count = 0
    print ("  Trying to establish initial connection to the server...")
    while success is None:
        conn = psy.connect(host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR)
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

peer_df = get_data(GITHUB+'/''scripts/2019/prework_queries/id7001_peers.sql')
peer_df.to_csv(GITHUB+'/''data/id7001_peers.csv',index=False)
extrap_df = get_data(GITHUB+'/''scripts/2019/prework_queries/id7001_extrap_numbers.sql')
extrap_df.to_csv(GITHUB+'/''data/id7001_extrap_numbers.csv',index=False)

## changing max year to 2025
peer_df['primary_new_contract_start_date'] = np.where(peer_df['primary_new_contract_start_date']>2025,2025,peer_df['primary_new_contract_start_date'])
extrap_df['primary_new_contract_start_date'] = np.where(extrap_df['primary_new_contract_start_date']>2025,2025,extrap_df['primary_new_contract_start_date'])

cost_insights1 = pd.read_csv(GITHUB+'''data/id5001_cost_projections_2015_2025.csv')
cost_insights1 = cost_data_transform(cost_insights1)

## current pricing
plot, df_current_pricing = plot_1mbps_future_model(cost_data= cost_insights1, 
	peer_data=peer_df, 
	extrap_data=extrap_df, 
	cost_projection_type = 'current_pricing', 
	cost_group = 'national', 
	same_sp_in_contract = False, 
	same_sp_preference = True, 
	units = 'districts')

plot.savefig(GITHUB+'/''figure_images/'+os.path.basename(__file__).replace('.py','.png'))
