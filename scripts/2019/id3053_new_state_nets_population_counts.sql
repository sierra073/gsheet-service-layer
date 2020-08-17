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
and dffa.fit_for_ia = true
),
dirty_distos as(
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
,
calcos as (
select ds.state_net,
  ds.size,
  sum(case when ds.meeting_2018_goal_oversub = true
  then 1 else 0 end) as dist_meeting,
  count(ds.district_id) as dist_total,
  sum(case when ds.meeting_2018_goal_oversub = true
  then ds.num_students else 0 end) as students_meeting,
  sum(ds.num_students) as students_total


from distos ds

group by ds.state_net
, ds.size
  
order by ds.state_net
, ds.size
), 

--select * from calcos

ratioz as (
select state_net,
size,
(dist_meeting::numeric / dist_total::numeric) as district_ratio,
(students_meeting::numeric/students_total::numeric) as student_ratio

from calcos cal
)

select dd.state_net,
  dd.size,
  count(dd.district_id) * r.district_ratio as dist_extrap,
  sum(dd.num_students) * r.student_ratio as students_extrap

from dirty_distos dd

join ratioz r
on r.state_net = dd.state_net
and r.size = dd.size

group by dd.state_net
, dd.size
, r.district_ratio
, r.student_ratio
  
order by dd.state_net
, dd.size