--State Network States, how many districts have pop >1000?
with
--states_static_temp as ( --new state network indicator
--select *,
--case when ss.state_code in ('AL', 'AR', 'CT', 'DE', 'GA', 'ME', 'NC', 'ND', 'NE', 'RI', 'SC', 'SD', 'UT', 'WA', 'WY', 'WV')
  --then true
  --when ss.state_code in  ('KY','MS')
  --then null
  --else false
  --end as state_network_new

--from ps.states_static ss),
--total districts for population categories
totalsa as (
select count(distinct d.district_id) as tot_districts,
  sum(case when d.locale = 'Urban'
  then 1 else 0
  end) as urb_dists,
  sum(case when d.locale = 'Suburban'
  then 1 else 0
  end) as suburb_dists,
  sum(case when d.locale = 'Town'
  then 1 else 0
  end) as town_dists,
  sum(case when d.locale = 'Rural'
  then 1 else 0
  end) as rural_dists,
  true as st_net,
  1 as d_vs_stu --1 for district 0 for student pop
  
  
from ps.districts d

join ps.states_static ss
on ss.state_code = d.state_code

where ss.state_network_natl_analysis = true
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'
),
--total districts for non-network states
totalsb as (
select count(distinct d.district_id) as tot_districts,
  sum(case when d.locale = 'Urban'
  then 1 else 0
  end) as urb_dists,
  sum(case when d.locale = 'Suburban'
  then 1 else 0
  end) as suburb_dists,
  sum(case when d.locale = 'Town'
  then 1 else 0
  end) as town_dists,
  sum(case when d.locale = 'Rural'
  then 1 else 0
  end) as rural_dists,
  false as st_net,
  1 as d_vs_stu --1 for district 0 for student pop
  
from ps.districts d

join ps.states_static ss
on ss.state_code = d.state_code

where (ss.state_network_natl_analysis = false or ss.state_network_natl_analysis is null)
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'
)

select *
from totalsa

UNION --add count of districts meeting 2018 goal

select sum(case when (dbc.meeting_2018_goal_oversub = true) --all districts meeting
      then 1 else 0
      end) as total_meeting,
      sum(case when (dbc.meeting_2018_goal_oversub = true and d.locale = 'Urban') --urban meeting
      then 1 else 0
      end) as urban_meeting,
      sum(case when dbc.meeting_2018_goal_oversub = true and d.locale = 'Suburban' --suburban meeting
      then 1 else 0
      end) as suburb_meeting,
      sum(case when (dbc.meeting_2018_goal_oversub = true and d.locale = 'Town') --town meeting
      then 1 else 0
      end) as town_meeting,
      sum(case when (dbc.meeting_2018_goal_oversub = true and d.locale = 'Rural') --rural meeting
      then 1 else 0
      end) as rural_meeting,
      true as st_net,
      1 as d_vs_stu --1 for district 0 for student pop

from ps.districts d

join ps.states_static ss --new state network indicator
on ss.state_code = d.state_code

join ps.districts_bw_cost dbc
on dbc.district_id = d.district_id
and dbc.funding_year = d.funding_year

where ss.state_network_natl_analysis = true
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'

union --add student population for the 4 categories, network = true

select sum(d.num_students) as tot_pop,
  sum(case when d.locale = 'Urban' 
    then d.num_students else 0 end) as urban_pop,
  sum(case when d.locale = 'Suburban'
    then d.num_students else 0 end) as suburb_pop,
  sum(case when d.locale = 'Town'
    then d.num_students else 0 end) as town_pop,
  sum(case when d.locale = 'Rural'
    then d.num_students else 0 end) as rural_pop,
  true as st_net,
  0 as d_vs_stu --1 for district 0 for student pop
  

from ps.districts d

join ps.states_static ss --new state network indicator
on ss.state_code = d.state_code

where ss.state_network_natl_analysis = true
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'

union -- add student population in districts that are meeting goal

select sum(case when dbc.meeting_2018_goal_oversub =true
  then d.num_students else 0 end) as tot_pop,
  sum(case when (d.locale = 'Urban' and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as urban_meeting,
  sum(case when (d.locale = 'Suburban' and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as suburb_meeting,
  sum(case when (d.locale = 'Town' and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as town_meeting,
  sum(case when (d.locale = 'Rural' and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as rural_meeting,
  true as st_net,
  0 as d_vs_stu --1 for district 0 for student pop
  

from ps.districts d

join ps.states_static ss --new state network indicator
on ss.state_code = d.state_code

join ps.districts_bw_cost dbc
on dbc.district_id = d.district_id
and dbc.funding_year = d.funding_year

where ss.state_network_natl_analysis = true --new state network indicator
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'

UNION
--Non-Network figures

select *
from totalsb

UNION --add count of districts meeting 2018 goal

select sum(case when (dbc.meeting_2018_goal_oversub = true) --all districts meeting
      then 1 else 0
      end) as total_meeting,
      sum(case when (dbc.meeting_2018_goal_oversub = true and d.locale = 'Urban') --urban meeting
      then 1 else 0
      end) as urban_meeting,
      sum(case when (dbc.meeting_2018_goal_oversub = true and d.locale = 'Suburban') --suburban meeting
      then 1 else 0
      end) as suburb_meeting,
      sum(case when (dbc.meeting_2018_goal_oversub = true and d.locale = 'Town') --town meeting
      then 1 else 0
      end) as town_meeting,
      sum(case when (dbc.meeting_2018_goal_oversub = true and d.locale = 'Rural') --rural meeting
      then 1 else 0
      end) as rural_meeting,
      false as st_net,
      1 as d_vs_stu --1 for district 0 for student pop

from ps.districts d

join ps.states_static ss --new state network indicator
on ss.state_code = d.state_code

join ps.districts_bw_cost dbc
on dbc.district_id = d.district_id
and dbc.funding_year = d.funding_year

where (ss.state_network_natl_analysis = false or ss.state_network_natl_analysis is null)
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'

union --add student population for the 4 categories (non-networks)

select sum(d.num_students) as tot_pop,
  sum(case when d.locale = 'Urban' 
    then d.num_students else 0 end) as urban_pop,
  sum(case when d.locale = 'Suburban'
    then d.num_students else 0 end) as suburb_pop,
  sum(case when d.locale = 'Town'
    then d.num_students else 0 end) as town_pop,
  sum(case when d.locale = 'Rural'
    then d.num_students else 0 end) as rural_pop,
  false as st_net,
  0 as d_vs_stu --1 for district 0 for student pop
  

from ps.districts d

join ps.states_static ss --new state network indicator
on ss.state_code = d.state_code

where (ss.state_network_natl_analysis = false or ss.state_network_natl_analysis is null)
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'

union -- add student population in NON-NETWORK districts that are meeting goal

select sum(case when dbc.meeting_2018_goal_oversub =true
  then d.num_students else 0 end) as tot_pop,
  sum(case when (d.locale = 'Urban' and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as urban_meeting,
  sum(case when (d.locale = 'Suburban' and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as suburb_meeting,
  sum(case when (d.locale = 'Town' and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as town_meeting,
  sum(case when (d.locale = 'Rural' and dbc.meeting_2018_goal_oversub =true)
  then d.num_students else 0
  end) as rural_meeting,
  false as st_net,
  0 as d_vs_stu --1 for district 0 for student pop
  

from ps.districts d

join ps.states_static ss --new state network indicator
on ss.state_code = d.state_code

join ps.districts_bw_cost dbc
on dbc.district_id = d.district_id
and dbc.funding_year = d.funding_year

where (ss.state_network_natl_analysis = false or ss.state_network_natl_analysis is null) --new state network indicator
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'