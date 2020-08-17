with national_median as (
select
  median(dbc.ia_monthly_cost_per_mbps) as median_current_18
  
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
and d.district_type = 'Traditional')

select (22 - median_current_18)::numeric/22 as ia_cost_mbps_decrease
from national_median