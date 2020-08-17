import os
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import sys
import pandas as pd
import numpy as np

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")

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

not_meeting_extrap = get_data(GITHUB+'/''scripts/2019/prework_queries/id7030_extrap_not_meeting_including_wi.sql')
wi_districts = get_data(GITHUB+'/''scripts/2019/prework_queries/id7030_best_deals_wi.sql')

all_no_spiya = pd.read_csv(GITHUB+'/''data/id7024_all_districts.csv')
best_deals = pd.read_csv(GITHUB+'/''data/id7024_best_deal_districts.csv')

best_deal_col = ['funding_year','state_code','district_id','num_students','old_total_bw','old_bw_per_student','new_total_bw','new_bw_per_student','total_bw_change','total_bw_percent_change']

## updates best deal csv with WI best deal/peer district state deal
best_deals_including_wi = pd.concat([best_deals[best_deal_col],wi_districts[best_deal_col][wi_districts.best_deal == True]])
best_deals_including_wi.to_csv(GITHUB+'/''data/id7030_best_deal_districts_including_wi.csv',index=False)

## updates all districts (that can't get SPIYAs) csv with WI best deal/peer district state deal
all_no_spiya_col = ['funding_year','state_code','district_id','num_students','best_deal']
all_no_spiya_including_wi = pd.concat([all_no_spiya[all_no_spiya_col],wi_districts[all_no_spiya_col]])
all_no_spiya_including_wi.to_csv(GITHUB+'/''data/id7030_all_districts_including_wi.csv',index=False)

### extrapolates including WI/peer district state #s
not_meeting_extrap['district_no_new_deals_extrap'] = (not_meeting_extrap.extrapolated_districts_not_meeting - not_meeting_extrap.districts_w_deals_extrap).astype(float)
not_meeting_extrap['students_no_new_deals_extrap'] = (not_meeting_extrap.extrapolated_students_not_meeting - not_meeting_extrap.students_w_deal_extrap).astype(float)
districts_agg = all_no_spiya_including_wi.groupby(['best_deal','funding_year']).agg({'district_id':len,'num_students':sum}).reset_index()
fy_agg = all_no_spiya_including_wi.groupby(['funding_year']).agg({'district_id':len,'num_students':sum}).reset_index()
fy_agg.rename(columns={'district_id':'funding_year_districts','num_students':'funding_year_students'},inplace=True)
districts_agg = pd.merge(districts_agg,fy_agg, on = 'funding_year')
districts_agg['districts_percent'] = districts_agg.district_id/districts_agg.funding_year_districts
districts_agg['students_percent'] = districts_agg.num_students/districts_agg.funding_year_students
districts_agg = pd.merge(districts_agg,not_meeting_extrap, on = ['funding_year'])
districts_agg['extrap_districts'] = (districts_agg.districts_percent*districts_agg.district_no_new_deals_extrap).round()
districts_agg['extrap_students_million'] = ((districts_agg.students_percent*districts_agg.students_no_new_deals_extrap)/1000000).round(2)
districts_agg = districts_agg[['funding_year','best_deal','districts_percent','extrap_districts','students_percent','extrap_students_million']]
districts_agg.to_csv(GITHUB+'/''data/'+os.path.basename(__file__).replace('.py','.csv'),index=False)
