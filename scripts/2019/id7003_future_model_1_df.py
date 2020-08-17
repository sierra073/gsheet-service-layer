import os
from dotenv import load_dotenv, find_dotenv
import sys

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")

sys.path.insert(0, GITHUB + "/''scripts/2019/prework_queries")
from id7001_model_functions import *

peer_df = pd.read_csv(GITHUB+'''data/id7001_peers.csv')
extrap_df = pd.read_csv(GITHUB+'''data/id7001_extrap_numbers.csv')

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

df_current_pricing.to_csv(GITHUB+'/''data/'+os.path.basename(__file__).replace('.py','.csv'),index=False)
