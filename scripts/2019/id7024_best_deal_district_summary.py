############################## get_data.py ##############################
import os
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import sys
import pandas as pd
import numpy as np
import math

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")

#HOST_DAR = os.environ.get("HOST_DAR")
#USER_DAR = os.environ.get("USER_DAR")
#PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
#DB_DAR = os.environ.get("DB_DAR")

HOST_DAR = "charizard-psql1.cyttrh279zkr.us-east-1.rds.amazonaws.com"
USER_DAR = os.environ.get("USER_SPINER")
PASSWORD_DAR = os.environ.get("PASSWORD_SPINER")
DB_DAR = "sots_snapshot_2019_08_19"

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

primary_districts = get_data(GITHUB+'/Projects/peer_deal_line_items/src/sql/primary_districts.sql')
sp_mapping_esh = get_data(GITHUB+'/Projects/peer_deal_line_items/src/sql/service_provider_mapping_esh.sql')
all_potential_deals = get_data(GITHUB+'/Projects/peer_deal_line_items/src/sql/all_potential_deals.sql')

## get geotel service provider data
conn = psy.connect(host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR)
cur = conn.cursor()
cur.execute("select * from dl.geotel_service_provider_static")
names = [x[0] for x in cur.description]
rows = cur.fetchall()
sp_mapping_geotel = pd.DataFrame(rows, columns=names)


## limiting to districts that don't already have a no cost/service provider in your area deal
## and are also in line_item deal states to speed things up
no_deals = get_data(GITHUB+'/Projects/sots-isl/scripts/2019/prework_queries/id7024_district_info.sql')
primary_districts = primary_districts[primary_districts.primary_id.isin(no_deals.district_id)]

############################# munge_data.py ##############################
print('  Munging deals')

## convert columns
primary_numeric_columns = ['primary_projected_bw_fy2018','primary_ia_monthly_cost_total','primary_max_cost_per_mbps_needed']
primary_districts[primary_numeric_columns] = primary_districts[primary_numeric_columns].astype(float)

deals_numeric_columns = ['bandwidth_in_mbps','circuit_total_monthly_cost','circuit_cost_per_mbps']
all_potential_deals[deals_numeric_columns] = all_potential_deals[deals_numeric_columns].astype(float)

## refine potential deals based on ESH Service Provider Mapping
limit_deals_esh = pd.merge(sp_mapping_esh, all_potential_deals, left_on = ['primary_state','funding_year','parent_name','sp_connect_category'], right_on = ['state_code','funding_year','parent_name','connect_category'], how ='inner')
limit_deals_esh.drop_duplicates(inplace = True)

## Incorporate geotel data
## limit geotel data to providers within a mile of the school
sp_mapping_geotel = sp_mapping_geotel[sp_mapping_geotel.miles_to_school <= 1]
## geotel only identifies service providers near district that provide fiber
sp_mapping_geotel['sp_connect_category'] = 'Fiber'
## add funding_year (duplicate table for 2018)
sp_mapping_geotel['funding_year'] = 2019
sp_mapping_geotel_2018 = sp_mapping_geotel.copy()
sp_mapping_geotel_2018['funding_year'] = 2018
sp_mapping_geotel = pd.concat([sp_mapping_geotel_2018, sp_mapping_geotel], axis=0, ignore_index=True)
## remove any geotel district-service provider mappings that are also in our data since what we have is more complete
sp_mapping_geotel = sp_mapping_geotel[['district_id','funding_year','esh_sp','sp_connect_category']].copy()
sp_mapping_esh['lowercase_join_name'] = sp_mapping_esh.parent_name.apply(str.lower)
sp_mapping_geotel_fix = pd.merge(sp_mapping_geotel,sp_mapping_esh,left_on = ['district_id','funding_year','esh_sp','sp_connect_category'],right_on = ['primary_id','funding_year','lowercase_join_name','sp_connect_category'],how = 'left',indicator=True)

esh_geotel_overlap = sp_mapping_geotel_fix[sp_mapping_geotel_fix._merge == 'both']
sp_mapping_geotel_fix = sp_mapping_geotel_fix[['district_id','funding_year','esh_sp','sp_connect_category']][sp_mapping_geotel_fix._merge == 'left_only']

## get state info for primary district and merge with geotel data
state_match = sp_mapping_esh[['primary_id','funding_year','primary_state','primary_hierarchy_ia_connect_category']].copy()
state_match.drop_duplicates(keep = 'first', inplace = True)

## this will remove districts in geotel csv that don't receive any direct services from a provider other than non e-rate "owned" provider (district, IU, etc)
sp_mapping_geotel_fix = pd.merge(sp_mapping_geotel_fix,state_match, left_on = ['district_id','funding_year'],right_on = ['primary_id','funding_year'], how = 'inner')

## refine potential deals based on Geotel Service Provider Mapping, State, Funding Year & Fiber
all_potential_deals['lowercase_join_name'] = all_potential_deals.parent_name.apply(str.lower)
limit_deals_geotel = pd.merge(sp_mapping_geotel_fix,all_potential_deals, left_on = ['primary_state','funding_year','esh_sp','sp_connect_category'], right_on = ['state_code','funding_year','lowercase_join_name','connect_category'],how = 'inner')

## combine deals
colnames = limit_deals_esh.columns
limit_deals_geotel = limit_deals_geotel[colnames]

limit_deals_esh['mapping_source'] = 'esh'
limit_deals_geotel['mapping_source'] = 'geotel'

limit_deals = pd.concat([limit_deals_esh,limit_deals_geotel])

## merge in all primary district data
limit_deals = pd.merge(primary_districts,limit_deals, on = ['primary_id','funding_year','primary_state','primary_hierarchy_ia_connect_category'], how = 'inner')
limit_deals.drop(columns=['state_code','sp_connect_category'],inplace=True)

## removing any deals that are more expensive than the districts budget
limit_deals = limit_deals[limit_deals.circuit_total_monthly_cost <= limit_deals.primary_ia_monthly_cost_total].copy()

## deal type scenario 1 - only find deals within one provider
## find best deals within each provider group
limit_deals_1_provider = limit_deals.copy()
limit_deals_1_provider['concat_unique_group_id'] = limit_deals_1_provider.primary_id.astype(str) + '-' + limit_deals_1_provider.funding_year.astype(str) + '-' + limit_deals_1_provider.parent_name

def rank_and_remove(df):
    ## rank line items within a peer by the highest bandwidth and most expensive
	df['bw_rank'] = df.groupby(['concat_unique_group_id'])['bandwidth_in_mbps'].rank(method = 'min', ascending=False)
	df['cost_rank'] = df.groupby(['concat_unique_group_id'])['circuit_total_monthly_cost'].rank(method = 'min', ascending=False)

	## remove any deals where it is lower (large rank number) in the bandwith ranking than it is in cost ranking
	## e.g. there is a line item with more bandwidth that costs less/costs the same as it does
	df = df[df.bw_rank <= df.cost_rank].copy()

	## rerank deals now that some have been removed
	df['bw_rank'] = df.groupby(['concat_unique_group_id'])['bandwidth_in_mbps'].rank(method = 'min', ascending=False)
	df['cost_rank'] = df.groupby(['concat_unique_group_id'])['circuit_total_monthly_cost'].rank(method = 'min', ascending=False)

	return df

## rank and remove deals in the scenario where theres is more than one connection type for the same bw and cost
## e.g. if there is a 50 Mbps DSL and Cable connection at $99/month
## connection ranking order based on hierarchy connect category logic
## also removes scenario where there is more than on connection type for same bw, but different cost and connection type
def rank_connect_type(connect_type):
    if connect_type == 'Satellite/LTE':
        return 1
    elif connect_type == 'T-1':
        return 2
    elif connect_type == 'Other Copper':
        return 3
    elif connect_type == 'DSL':
        return 4
    elif connect_type == 'Cable':
        return 5
    elif connect_type == 'Fixed Wireless':
    	return 6
    elif connect_type == 'Fiber':
        return 7

def rank_and_remove_connect_type(df):
    df['rank_connect_category']= df.connect_category.apply(rank_connect_type).astype('int64')
    df['best_connect_type'] = df.groupby(['concat_unique_group_id','bandwidth_in_mbps','circuit_total_monthly_cost'])['rank_connect_category'].rank(method = 'min',ascending = False)
    df['cheapest_bw'] = df.groupby(['concat_unique_group_id','bandwidth_in_mbps'])['circuit_total_monthly_cost'].rank(method = 'min', ascending=True)
    df = df[(df.best_connect_type == 1)&(df.cheapest_bw == 1)].copy()
    return df

limit_deals_1_provider = rank_and_remove_connect_type(limit_deals_1_provider)

limit_deals_1_provider = rank_and_remove(limit_deals_1_provider)
limit_deals_1_provider = rank_and_remove(limit_deals_1_provider)
limit_deals_1_provider = rank_and_remove(limit_deals_1_provider)

## deal type scenario 2 - find deals across all providers
count_sps = limit_deals_1_provider.groupby(['primary_id', 'funding_year'])['parent_name'].nunique().reset_index()
limit_deals_all_provider = limit_deals_1_provider.merge(count_sps, on=['primary_id', 'funding_year']).rename(columns={'parent_name_x': 'parent_name', 'parent_name_y': 'num_sps'})
limit_deals_all_provider = limit_deals_all_provider[limit_deals_all_provider.num_sps > 1]
limit_deals_all_provider['concat_unique_group_id'] = limit_deals_all_provider.primary_id.astype(str) + '-' + limit_deals_all_provider.funding_year.astype(str) + '-All Providers'
limit_deals_all_provider.drop(columns=['bw_rank','cost_rank', 'num_sps'],inplace=True)

groupby_columns = list(limit_deals_all_provider.columns.values)
groupby_columns.remove('mapping_source')
groupby_columns.remove('line_item_id')
groupby_columns.remove('parent_name')

limit_deals_all_provider = limit_deals_all_provider.groupby(groupby_columns, as_index=False).agg({'parent_name':lambda x: "%s" % '; '.join(x),
                                                       'line_item_id':lambda x: "%s" % '; '.join(x),
                                                       'mapping_source':lambda x: "%s" % '; '.join(np.unique(x))})
limit_deals_all_provider = pd.DataFrame(limit_deals_all_provider)

limit_deals_all_provider = rank_and_remove_connect_type(limit_deals_all_provider)

limit_deals_all_provider = rank_and_remove(limit_deals_all_provider)
limit_deals_all_provider = rank_and_remove(limit_deals_all_provider)
limit_deals_all_provider = rank_and_remove(limit_deals_all_provider)

## remove deals from limit_deals_all_provider if after all the filtering there ends up being only one service provider
count_sps = limit_deals_all_provider.groupby(['primary_id', 'funding_year'])['parent_name'].nunique().reset_index()
limit_deals_all_provider = limit_deals_all_provider.merge(count_sps, on=['primary_id', 'funding_year']).rename(columns={'parent_name_x': 'parent_name', 'parent_name_y': 'num_sps'})
limit_deals_all_provider = limit_deals_all_provider[limit_deals_all_provider.num_sps > 1]

## join together both limit deal dfs
colnames = limit_deals_1_provider.columns
limit_deals_all_provider = limit_deals_all_provider[colnames]

limit_deals_1_provider['deal_type'] = 'limited_within_provider'
limit_deals_all_provider['deal_type'] = 'all_providers'
limit_deals = pd.concat([limit_deals_1_provider,limit_deals_all_provider])

limit_deals.sort_values(by=['concat_unique_group_id','bw_rank'], ascending=True, inplace= True)

############################## get_best_deals.py ###############################
print('  Getting the best deals. Takes a few minutes.')
limit_deals = limit_deals[limit_deals.funding_year == 2019].copy()

def get_deals(deal_df):

    primary_and_sp_df = deal_df[['concat_unique_group_id','primary_projected_bw_fy2018','primary_ia_monthly_cost_total']].copy()
    primary_and_sp_df.drop_duplicates(keep = 'first', inplace = True)

    results_df = pd.DataFrame(columns = {'concat_unique_group_id','bw','cost','circuits','deal_worked'})

    for id, primary_budget in zip(primary_and_sp_df.concat_unique_group_id,
                                                    primary_and_sp_df.primary_ia_monthly_cost_total):
        budget_still_left = primary_budget
        df_temp = deal_df[deal_df.concat_unique_group_id == id].copy()
        ##results = {}
        for concat_unique_group_id, bw, cost in zip(df_temp.concat_unique_group_id, df_temp.bandwidth_in_mbps,df_temp.circuit_total_monthly_cost):
            if (budget_still_left<=0):
                pass
            else:
                ## how many circuits of this deal would they need to get all of their bw
                # max_circuits_bw = math.ceil(bw_still_needed/bw)
                ## how many circuits can they afford
                max_circuits_cost = math.floor(budget_still_left/cost)
                circuits = min(max_circuits_cost,4)  # min(max_circuits_bw,max_circuits_cost)
                # bw_still_needed = bw_still_needed - (circuits*bw)
                budget_still_left = budget_still_left - (circuits*cost)
                if budget_still_left >= 0:
                    deal_worked = True
                else:
                    deal_worked = False
                results_df = results_df.append({'concat_unique_group_id': concat_unique_group_id, 'bw':bw,'cost':cost,'circuits':circuits,'deal_worked':deal_worked},ignore_index=True)

    ## remove all deals if none of them "worked" by getting the primary all the BW it needed while staying within budget
    results_df = results_df[results_df.concat_unique_group_id.isin(results_df.concat_unique_group_id[results_df.deal_worked==True])]
    results_df = results_df[results_df.circuits > 0]
    return results_df

actual_deals = get_deals(limit_deals)

## remove any deals that involve district procuring more than 4 circuits
#actual_deals = actual_deals[~actual_deals.concat_unique_group_id.isin(actual_deals.concat_unique_group_id[actual_deals.circuits>4])]

actual_deals_info = pd.merge(limit_deals,actual_deals,left_on = ['concat_unique_group_id','bandwidth_in_mbps','circuit_total_monthly_cost'], right_on= ['concat_unique_group_id','bw','cost'])

## limit deal type of all providers to where the count of unique service providers is 2
## if there is only 1 unique service provider then it is a duplicate of the limited_within_provider type
## if it's more than 2 unique service providers we decided that's too many for a district to get deals from
count_sps = actual_deals_info[actual_deals_info.deal_type == 'all_providers'].groupby(['primary_id'],as_index=True)['parent_name'].nunique()
actual_deals_info = actual_deals_info[(actual_deals_info.deal_type == 'limited_within_provider')|((actual_deals_info.deal_type == 'all_providers')&(actual_deals_info.primary_id.isin(count_sps.index[count_sps==2])))].copy()

## create unique ID for deals
actual_deals_info['total_li_cost'] = actual_deals_info.circuit_total_monthly_cost * actual_deals_info.circuits
deal_summary = actual_deals_info.groupby(['primary_id','concat_unique_group_id'],as_index=False)['total_li_cost'].sum()
deal_summary['cheapest_deal'] = deal_summary.groupby('primary_id')['total_li_cost'].rank(method = 'first', ascending=True)
deal_summary['deal'] = 'Deal ' + deal_summary.cheapest_deal.astype(int).astype(str)
actual_deals_info = pd.merge(actual_deals_info,deal_summary, on = ['primary_id','concat_unique_group_id'])

## cleanup columns
actual_deals_info.rename(columns={'primary_id':'district_id','line_item_id':'all_line_item_ids','connect_category':'connect_category_modified'},inplace= True)
## selecting one of the line items from the array to be the main/unique line item id
actual_deals_info['line_item_id'] = actual_deals_info.all_line_item_ids.apply(lambda x: x.split(';')[0])

## more column cleanup
final_cols = ['funding_year','district_id','deal','line_item_id','all_line_item_ids','parent_name','circuits','bandwidth_in_mbps','circuit_total_monthly_cost','connect_category_modified','mapping_source','deal_type']
best_deals = actual_deals_info[final_cols].copy()

int_cols = ['funding_year','district_id','line_item_id','circuits']
numeric_cols = ['bandwidth_in_mbps','circuit_total_monthly_cost']
best_deals[int_cols] = best_deals[int_cols].astype(int)
best_deals[numeric_cols] = best_deals[numeric_cols].astype(float)

############################## best_deal_analysis.py ##############################
print('  Summarizing data')
not_meeting_extrap = get_data(GITHUB+'/Projects/sots-isl/scripts/2019/prework_queries/id7024_extrap_not_meeting.sql')
district_no_new_deals_extrap = float(not_meeting_extrap.iloc[0]['extrapolated_districts_not_meeting'] - not_meeting_extrap.iloc[0]['districts_w_deals_extrap'])
students_no_new_deals_extrap = float(not_meeting_extrap.iloc[0]['extrapolated_students_not_meeting'] - not_meeting_extrap.iloc[0]['students_w_deal_extrap'])

## already queried this at beginning to remove districts that already have no cost deal/service provider in your area deal
#no_deals = get_data(GITHUB+'/Projects/sots-isl/scripts/2019/prework_queries/id7024_district_info.sql')
float_columns = ['ia_bw_mbps_total','ia_monthly_cost_total','ia_bandwidth_per_student_kbps','projected_bw_fy2018']
no_deals[float_columns] = no_deals[float_columns].astype(float)

best_deals['total_bw'] = best_deals.bandwidth_in_mbps * best_deals.circuits
best_deals['total_cost'] = best_deals.circuit_total_monthly_cost * best_deals.circuits

agg_best_deals = best_deals.groupby(['funding_year','district_id','deal']).agg({'line_item_id':len,'circuits':sum,'total_bw':sum,'total_cost':sum}).reset_index()
agg_best_deals.rename(columns={'line_item_id':'total_li','circuits':'total_circuits'},inplace=True)

## sorting by cost so that when i rank by bandwidth the cheaper deals at the same total bandwidth will be ranked higher
agg_best_deals.sort_values(by=['funding_year','district_id','total_bw','total_cost'],ascending=True,inplace=True)
agg_best_deals['bw_rank'] = agg_best_deals.groupby(['funding_year','district_id'])['total_bw'].rank(method='first',ascending = False)
## picking the deal with the most bandwidth
agg_best_deals = agg_best_deals[agg_best_deals.bw_rank == 1]

## merging in extra info and also removing any districts that already have regular deals
districts = pd.merge(no_deals,agg_best_deals, on = ['district_id','funding_year'],how='left')
## remove deals that don't work
districts['best_deal'] = np.where(districts.total_bw.isna(),False,
	np.where(districts.total_bw > districts.ia_bw_mbps_total,True,False))

#### saving all district data ###
districts.to_csv(GITHUB+'/Projects/sots-isl/data/id7024_all_districts.csv',index=False)

best_deal_districts = districts[districts['best_deal'] == True].copy()
best_deal_districts.rename(columns={'ia_bw_mbps_total':'old_total_bw','ia_bandwidth_per_student_kbps':'old_bw_per_student',
                                   'total_bw':'new_total_bw','ia_monthly_cost_total':'old_total_cost',
                                   'total_cost':'new_total_cost'},inplace=True)
best_deal_districts['new_bw_per_student'] = (best_deal_districts.new_total_bw*1000)/best_deal_districts.num_students
best_deal_districts['total_bw_change'] = (best_deal_districts.new_total_bw - best_deal_districts.old_total_bw)
best_deal_districts['total_bw_percent_change'] = best_deal_districts.total_bw_change/best_deal_districts.old_total_bw

#### saving best deal district data ####
best_deal_districts.to_csv(GITHUB+'/Projects/sots-isl/data/id7024_best_deal_districts.csv',index=False)

#### saving extrapolated district data ###
districts_agg = districts.groupby(['best_deal']).agg({'district_id':len,'num_students':sum}).reset_index()
districts_agg['districts_percent'] = districts_agg.district_id/districts_agg.district_id.sum()
districts_agg['students_percent'] = districts_agg.num_students/districts_agg.num_students.sum()
districts_agg['extrap_districts'] = (districts_agg.districts_percent*district_no_new_deals_extrap).round()
districts_agg['extrap_students_million'] = ((districts_agg.students_percent*students_no_new_deals_extrap)/1000000).round(2)
districts_agg = districts_agg[['best_deal','districts_percent','extrap_districts','students_percent','extrap_students_million']]
districts_agg.to_csv(GITHUB+'/Projects/sots-isl/data/'+os.path.basename(__file__).replace('.py','.csv'),index=False)
