with national_median as (
select
d.funding_year,
median(dbc.ia_monthly_cost_per_mbps) as median_cost_per_mbps
  
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
and d.state_code != 'AK'
and d.district_type = 'Traditional'
group by 1

union

select
d.funding_year,
median(dbc.ia_monthly_cost_per_mbps) as median_cost_per_mbps
  
from ps.districts_frozen_sots d

join ps.districts_bw_cost_frozen_sots dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_fit_for_analysis_frozen_sots dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

where d.funding_year = 2018
and dfa.fit_for_ia_cost = true
and d.state_code != 'AK'
and d.district_type = 'Traditional'
group by 1
)

select 
(c18.median_cost_per_mbps - c17.median_cost_per_mbps)::numeric/c17.median_cost_per_mbps
as cost_mbps_decline_17_18
from
(select * from national_median where funding_year=2019) c18
join
(select * from national_median where funding_year=2018) c17
on true