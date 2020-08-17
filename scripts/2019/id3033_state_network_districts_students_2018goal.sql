--State Network States, how many districts have pop >1000?
with totals as (
select count(distinct d.district_id) as tot_districts,
  sum(case when d.num_students <= 1000
  then 1 else 0
  end) as lessthan1k,
  sum(case when (d.num_students > 1000 and d.num_students <= 10000)
  then 1 else 0
  end) as onethousand_to_10k,
  sum(case when d.num_students > 10000
  then 1 else 0
  end) as over10k

from ps.districts d

join ps.states_static ss
on ss.state_code = d.state_code

where ss.state_network = true
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'
)

select *
from totals

UNION --add count of districts meeting 2018 goal

select sum(case when (dbc.meeting_2018_goal_oversub = true) --all districts meeting
      then 1 else 0
      end) as total_meeting,
      sum(case when (dbc.meeting_2018_goal_oversub = true and d.num_students <= 1000) --under 1000 meeting
      then 1 else 0
      end) as under1k_meeting,
      sum(case when dbc.meeting_2018_goal_oversub = true and d.num_students > 1000 and d.num_students <= 10000 --1k-10k meeting
      then 1 else 0
      end) as over9k_meeting,
      sum(case when (dbc.meeting_2018_goal_oversub = true and d.num_students > 10000) --over 10000 meeting
      then 1 else 0
      end) as over10k_meeting

from ps.districts d

join ps.states_static ss
on ss.state_code = d.state_code

join ps.districts_bw_cost dbc
on dbc.district_id = d.district_id
and dbc.funding_year = d.funding_year

where ss.state_network = true
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'

union --add student population for the 4 categories

select sum(d.num_students) as tot_pop,
  sum(case when d.num_students <= 1000
  then d.num_students else 0
  end) as lessthan1k,
  sum(case when (d.num_students > 1000 and d.num_students <= 10000)
  then d.num_students else 0
  end) as onethousand_to_10k,
  sum(case when d.num_students > 10000
  then d.num_students else 0
  end) as over10k
  

from ps.districts d

join ps.states_static ss
on ss.state_code = d.state_code

where ss.state_network = true
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'

union -- add student population in districts that are meeting goal

select sum(case when dbc.meeting_2018_goal_oversub =true
  then d.num_students else 0 end) as tot_pop,
  sum(case when (d.num_students <= 1000 and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as lessthan1k,
  sum(case when (d.num_students > 1000 and d.num_students <= 10000 and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as onethousand_to_10k,
  sum(case when (d.num_students > 10000 and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as over10k
  

from ps.districts d

join ps.states_static ss
on ss.state_code = d.state_code

join ps.districts_bw_cost dbc
on dbc.district_id = d.district_id
and dbc.funding_year = d.funding_year

where ss.state_network = true
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'
