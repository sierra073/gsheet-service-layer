with lookup as(
select 
  d.district_id, 
  d.state_code,
  d.name,
  d.funding_year, 
  d.latitude, 
  d.longitude, 
  d.num_students,
  d.locale,
  d.size,
  case when du.upgrade_indicator = false 
    then true
  else false end as boring_bw,
  
  case when df.fiber_target_status in ('Not Target','Potential Target')
  and df2.fiber_target_status = df.fiber_target_status
    then true
  else false end as boring_fiber,
  
  dbc.ia_monthly_cost_per_mbps as ia_monthly_cost_per_mbps,
  dbc2.ia_monthly_cost_per_mbps as ia_monthly_cost_per_mbps_prev,
  
  case when dbc2.ia_monthly_cost_per_mbps > dbc.ia_monthly_cost_per_mbps
    then true
  else false end as cost_decrease_indicator,
  
  case when dbc2.ia_monthly_cost_per_mbps > 0 
  then abs(dbc.ia_monthly_cost_per_mbps - dbc2.ia_monthly_cost_per_mbps)/dbc2.ia_monthly_cost_per_mbps::numeric end  as cost_per_mbps_change,
  
  case when dbc.meeting_knapsack_affordability_target != dbc2.meeting_knapsack_affordability_target
    then true
  else false end as knapsack_change

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

join ps.districts_upgrades  du
on d.district_id= du.district_id
and d.funding_year = du.funding_year

join ps.districts_fiber df
on d.district_id= df.district_id
and d.funding_year = df.funding_year

join ps.districts_fiber df2
on d.district_id= df2.district_id
and d.funding_year = df2.funding_year + 1

where d.funding_year = 2018
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and dfa2.fit_for_ia_cost = true
and d.state_code != 'AK'),

lookup_final as (
select district_id, 
locale, size, 
cost_decrease_indicator, knapsack_change, 
case when boring_bw and boring_fiber
  then true
else false end as boring_district,
cost_per_mbps_change,

case when cost_per_mbps_change > 
(select percentile_cont(.85) within group (order by cost_per_mbps_change) 
from lookup) 
  then true
else false end as cost_per_mbps_change_g85

from lookup)

select boring_district::varchar as boring_district_group, cost_per_mbps_change_g85, count(*) as ndistricts
from lookup_final
group by 1,2
union 
select 'all' as boring_district_group, cost_per_mbps_change_g85, count(*) as ndistricts
from lookup_final
group by 1,2
order by 1,2
