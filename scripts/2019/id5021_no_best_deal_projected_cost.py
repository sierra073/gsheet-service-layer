from __future__ import division

import os
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import sys
import pandas as pd
import numpy as np
import math

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")

HOST_DAR = "charizard-psql1.cyttrh279zkr.us-east-1.rds.amazonaws.com"
USER_DAR = os.environ.get("USER_SPINER")
PASSWORD_DAR = os.environ.get("PASSWORD_SPINER")
DB_DAR = "sots_snapshot_2019_08_19"

def get_data(sql):

    query = sql
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


sql = """
    select district_id,
      c1_discount_rate
    from ps.districts
    where funding_year = 2019
    and district_id in ({})
"""

sql_cost = """
    select district_id,
      projected_bw_fy2018,
      ia_monthly_cost_total,
      ia_bw_mbps_total
    from ps.districts_bw_cost
    where funding_year = 2019
    and district_id in ({})
"""

## to answer the first question of how many districts would meet given current deal and projections
df = pd.read_csv(GITHUB + '/''data/id7030_all_districts_including_wi.csv')

pop = df[(df.funding_year == 2019) & (df.best_deal == False)]
did_list1 = str(pop.district_id.tolist()).lstrip('[').rstrip(']')
cost_data = get_data(sql_cost.format(did_list1))
cost_data[['projected_bw_fy2018', 'ia_monthly_cost_total', 'ia_bw_mbps_total']] = cost_data[
    ['projected_bw_fy2018', 'ia_monthly_cost_total', 'ia_bw_mbps_total']].astype('float').round(2)

pop = pop.merge(cost_data, on='district_id', how='left').copy()

# if the bw needed is <= 1Gb and they are paying at least $1000
cond0 = (pop.projected_bw_fy2018 <= 1000) & (pop.ia_monthly_cost_total >= 1000)
# if the bw needed is <=2Gb and they are paying at least $2000
cond1 = (pop.projected_bw_fy2018 <= 2000) & (pop.ia_monthly_cost_total >= 2000)
# if the bw needed is > 2Gb < 10Gb and they are paying at least $2500
cond2 = (pop.projected_bw_fy2018 <= 10000) & (pop.ia_monthly_cost_total >= 2500)
# if the bw needed is >10Gb and they are paying at least $.25/Mbps
cond3 = (pop.projected_bw_fy2018 > 10000) & (pop.ia_monthly_cost_total/pop.projected_bw_fy2018 >= 0.25)

perc_districts_projected_meet_goals = round(len(pop[cond0 | cond1 | cond2 | cond3])/len(pop), 2)
perc_students_projected_meet_goals = round(pop.loc[cond0 | cond1 | cond2 | cond3, 'num_students'].sum()/pop.num_students.sum(), 2)

# how much more would districts need to pay to meet
pay_more = pop[~(cond0 | cond1 | cond2 | cond3)]

did_list2 = str(pay_more.district_id.tolist()).lstrip('[').rstrip(']')

disc_rates = get_data(sql.format(did_list2))
disc_rates['c1_discount_rate'] = disc_rates['c1_discount_rate'].astype('float').round(2)

pay_more = pay_more.merge(disc_rates, on='district_id', how='left').copy()

pay_more.loc[pay_more.projected_bw_fy2018 > 10000, 'goal_cost'] = pay_more.loc[pay_more.projected_bw_fy2018 > 10000,
                                                                               'projected_bw_fy2018']*.25
pay_more.loc[pay_more.projected_bw_fy2018 <= 10000, 'goal_cost'] = 2500
pay_more.loc[pay_more.projected_bw_fy2018 <= 2000, 'goal_cost'] = 2000
pay_more.loc[pay_more.projected_bw_fy2018 <= 1000, 'goal_cost'] = 1000

pay_more['add_cost_per_stud_post_discount'] = (pay_more.goal_cost - pay_more.ia_monthly_cost_total)*(1-pay_more.c1_discount_rate)/pay_more.num_students
weighted_avg_add_cost_per_student = round(pay_more['add_cost_per_stud_post_discount'].mean(), 2)

# output a csv of the original pop and new fields for Evan to double check
new_cols = ['district_id'] + [c for c in pay_more.columns if c not in pop.columns]

output = pop.merge(pay_more[new_cols], on='district_id', how='left')
output.to_csv(GITHUB + '/''data/id5021_no_best_deal_output.csv', index=False)
agg_output = pd.DataFrame({'perc_students_projected_meet_goals': [float(perc_students_projected_meet_goals)],
                           'weighted_avg_add_cost_per_student': [float(weighted_avg_add_cost_per_student)],
                           'alt_mean': round(((pay_more.goal_cost - pay_more.ia_monthly_cost_total)*(1-pay_more.c1_discount_rate)).sum()/pay_more.num_students.sum(), 2),
                           'median': round(pay_more['add_cost_per_stud_post_discount'].median(), 2)})

agg_output.to_csv(GITHUB + '/''data/id5021_no_best_deal_projected_cost.csv')
