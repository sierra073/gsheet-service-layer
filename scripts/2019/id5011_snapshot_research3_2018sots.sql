with lookup as(
select 
  d.state_code,
  
  (median(dbc.ia_monthly_cost_per_mbps) -
  median(dbc2.ia_monthly_cost_per_mbps))/
  median(dbc2.ia_monthly_cost_per_mbps)::numeric
  as median_cost_per_mbps_change
  
from ps.districts d

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_bw_cost dbc2
on d.district_id= dbc2.district_id
and d.funding_year = dbc2.funding_year + 1

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

join ps.districts_fit_for_analysis dfa2
on d.district_id= dfa2.district_id
and d.funding_year = dfa2.funding_year + 1

where d.funding_year = 2018
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and dfa2.fit_for_ia_cost = true
and d.state_code != 'AK'
group by 1)

select count(case when median_cost_per_mbps_change > 
(select percentile_cont(.9) within group (order by median_cost_per_mbps_change) 
from lookup) 
  then state_code end) as num_states_significant_change_median_cost_mbps
from lookup 

-- select state_code,
-- median_cost_per_mbps_change
-- from lookup
-- order by abs(median_cost_per_mbps_change) desc
