with distos as (
select d.district_id,
  d.size,
  d.locale,
  d.num_students,
  d.state_code,
  dbc.meeting_2018_goal_oversub,
  case when ss.peer_deal_type = 'line_items'
    then 'false'
    when ss.state_network_natl_analysis = true
    then 'true' 
    when ss.state_code = 'WI'
    then 'WI'
    else 'null' end as state_net

from ps.districts d

join ps.states_static ss
on ss.state_code = d.state_code

left join ps.districts_bw_cost dbc
on dbc.funding_year = d.funding_year
and dbc.district_id = d.district_id

left join ps.districts_fit_for_analysis dffa
on dffa.funding_year = d.funding_year
and dffa.district_id = d.district_id

where d.funding_year = 2019
--and ss.peer_deal_type = 'line_items'
and d.in_universe = true
and d.district_type = 'Traditional'
--and dffa.fit_for_ia = true
)

select ds.size,
  ds.state_net,
  count(distinct ds.district_id),
  sum(ds.num_students)
  
from distos ds

group by ds.size,
  ds.state_net
  
order by 
  ds.state_net,
  ds.size