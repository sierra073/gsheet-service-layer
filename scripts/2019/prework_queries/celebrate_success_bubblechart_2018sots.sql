select 
  d.funding_year,
  d.state_code,
  sum(d.num_students) as num_students,
  count(case when fit_for_ia = true and meeting_2014_goal_no_oversub = true then d.district_id end)::numeric/
  count(case when fit_for_ia = true then d.district_id end) as pct_districts_meeting,
  sum(case when fit_for_ia = true and meeting_2014_goal_no_oversub = true then num_schools else 0 end)::numeric/
  sum(case when fit_for_ia = true then num_schools end) as pct_schools_meeting,
  sum(case when fit_for_ia = true and meeting_2014_goal_no_oversub = true then num_campuses else 0 end)::numeric/
  sum(case when fit_for_ia = true then num_campuses end) as pct_campuses_meeting,
  sum(case when fit_for_ia = true and meeting_2014_goal_no_oversub = true then num_students else 0 end)::numeric/
  sum(case when fit_for_ia = true then num_students end) as pct_students_meeting,
  1 - (sum(known_unscalable_campuses + assumed_unscalable_campuses)::numeric/sum(num_campuses)) as pct_schools_on_fiber
  
from ps.districts d

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_fiber df
on d.district_id= df.district_id
and d.funding_year = df.funding_year

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

where d.in_universe = true
and d.district_type='Traditional'
and d.state_code != 'DC'
group by 1,2
having sum(num_campuses) > 0
and count(case when fit_for_ia = true then d.district_id end) > 0
order by 1,2