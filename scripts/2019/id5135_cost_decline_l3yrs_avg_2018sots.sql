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

where d.in_universe = true
and dfa.fit_for_ia = true
and dfa.fit_for_ia_cost = true
and d.state_code != 'AK'
and d.district_type = 'Traditional'
and d.funding_year = 2019 
group by 1

union 

select funding_year,
median_cost_per_mbps
from ps.state_snapshot_frozen_sots

where state_code = 'ALL'
),

final as (select 
(c18.median_cost_per_mbps - c17.median_cost_per_mbps)::numeric/c17.median_cost_per_mbps
as cost_mbps_decline_17_18,
(c17.median_cost_per_mbps - c16.median_cost_per_mbps)::numeric/c16.median_cost_per_mbps
as cost_mbps_decline_16_17,
(c16.median_cost_per_mbps - c15.median_cost_per_mbps)::numeric/c15.median_cost_per_mbps
as cost_mbps_decline_15_16
from
(select * from national_median where funding_year=2019) c18
join
(select * from national_median where funding_year=2018) c17
on true
join
(select * from national_median where funding_year=2017) c16
on true
join
(select * from national_median where funding_year=2015) c15
on true)

select (cost_mbps_decline_17_18 + cost_mbps_decline_16_17 + cost_mbps_decline_15_16)::numeric/3
as cost_decline_l3yrs_avg
from final