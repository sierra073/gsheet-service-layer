select
  median(dbc.ia_monthly_cost_per_mbps) as median_cost_per_mbps,
  sum(dbc.ia_monthly_cost_total)/sum(dbc.ia_bw_mbps_total) as weighted_avg_cost_mbps
  
from ps.districts d

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

where d.funding_year = 2019
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and d.district_type = 'Traditional'
and meeting_2018_goal_oversub = true
and state_code != 'AK'