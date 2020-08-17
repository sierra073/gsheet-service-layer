with t as (
--by State
select f17.state_code, 
  median_frozen_17,
  straight_avg_frozen_17,
  weighted_avg_frozen_17,
  median_current_17,
  straight_avg_current_17,
  weighted_avg_current_17,
  median_current_18,
  straight_avg_current_18,
  weighted_avg_current_18

from 
(select 
d.state_code, d.funding_year,
median(dbc.ia_monthly_cost_per_mbps)  as median_frozen_17,
avg(dbc.ia_monthly_cost_per_mbps) as straight_avg_frozen_17,
sum(dbc.ia_monthly_cost_total)/sum(dbc.ia_bw_mbps_total) as weighted_avg_frozen_17

from ps.districts_frozen_sots d

join ps.districts_bw_cost_frozen_sots dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_fit_for_analysis_frozen_sots dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

where d.funding_year = 2018
and dfa.fit_for_ia_cost = true
and dfa.fit_for_ia_cost = true
and d.district_type = 'Traditional'
group by 1,2) f17

join
(select
  d.state_code,
  d.funding_year,
  median(dbc.ia_monthly_cost_per_mbps) as median_current_17,
  avg(dbc.ia_monthly_cost_per_mbps) as straight_avg_current_17,
  sum(dbc.ia_monthly_cost_total)/sum(dbc.ia_bw_mbps_total) as weighted_avg_current_17
  
from ps.districts d

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

where d.funding_year = 2018
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and dfa.fit_for_ia_cost = true
and d.district_type = 'Traditional'
group by 1,2) c17

on f17.funding_year = c17.funding_year
and f17.state_code = c17.state_code

join
(select
  d.state_code,
  d.funding_year,
  median(dbc.ia_monthly_cost_per_mbps) as median_current_18,
  avg(dbc.ia_monthly_cost_per_mbps) as straight_avg_current_18,
  sum(dbc.ia_monthly_cost_total)/sum(dbc.ia_bw_mbps_total) as weighted_avg_current_18
  
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
and dfa.fit_for_ia_cost = true
and d.district_type = 'Traditional'
group by 1,2) c18

on c17.funding_year = c18.funding_year - 1
and c17.state_code = c18.state_code

order by state_code)

select count(*) from t 
where weighted_avg_current_18 <= 3
