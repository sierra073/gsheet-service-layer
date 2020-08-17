with percenties as(

with cost_breakdown as(

select d.district_id,
  d.state_code,
  ss.state_network,
  dbc.meeting_2018_goal_oversub,
  sum(dli.rec_cost) as total_mrc,
  case when dli.purpose = 'internet'
    then sum(dli.rec_cost)
    else 0 end as bia_cost,
  case when dli.purpose = 'wan'
    then sum(dli.rec_cost)
    else 0 end as wan_cost,
  case when dli.purpose = 'upstream'
    then sum(dli.rec_cost)
    else 0 end as upstream_cost,
  case when dli.purpose = 'isp'
    then sum(dli.rec_cost)
    else 0 end as isp_cost

from ps.districts_line_items dli

join ps.districts d
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

join ps.states_static ss
on ss.state_code = d.state_code

join ps.districts_bw_cost dbc
on dbc.funding_year = dli.funding_year
and dbc.district_id = dli.district_id

where dli.funding_year = 2019
and d.district_type= 'Traditional'
and d.in_universe = true
--and ss.state_network = true
and dli.rec_cost !=0

--and dli.district_id = 881423

group by d.district_id,
  dli.purpose,
  d.state_code,
  ss.state_network,
  dbc.meeting_2018_goal_oversub
)

select distinct cb.district_id,
  cb.state_code,
  cb.state_network,
  cb.meeting_2018_goal_oversub,
  sum(cb.total_mrc) as total_mrc,
  sum(cb.bia_cost) as bia_cost,
  sum(cb.wan_cost) as wan_cost,
  sum(cb.upstream_cost) as upstream_cost,
  sum(cb.isp_cost) as isp_cost

from cost_breakdown cb

--where total_mrc =0

group by cb.district_id,
  cb.state_code,
  cb.state_network,
  cb.meeting_2018_goal_oversub
)

select p.district_id,
p.state_code,
p.state_network,
p.meeting_2018_goal_oversub,
  p.total_mrc,
  p.bia_cost,
  (p.bia_cost/p.total_mrc) as bia_pct,
  p.wan_cost,
  (p.wan_cost/p.total_mrc) as wan_pct,
  p.upstream_cost,
  (p.upstream_cost/p.total_mrc) as upstream_pct,
  p.isp_cost,
  (p.isp_cost/p.total_mrc) as isp_pct

from percenties p