select 
  d.funding_year,
  sum(1::numeric) FILTER (where dbc.meeting_2018_goal_oversub = true
                          and d.size in ('Large', 'Mega'))/
    count(*) FILTER (where d.size in ('Large', 'Mega')) as pct_lrg_meeting,
  median(d.num_students) FILTER (where dbc.meeting_2018_goal_oversub = true) as median_student_pop,
  avg(d.num_students) FILTER (where dbc.meeting_2018_goal_oversub = true) as avg_student_pop,
  median(d.num_schools) FILTER (where dbc.meeting_2018_goal_oversub = true) as median_schools,
  avg(d.num_schools) FILTER (where dbc.meeting_2018_goal_oversub = true) as avg_schools
  
from ps.districts d

left join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

left join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and dfa.fit_for_ia = true

group by d.funding_year

