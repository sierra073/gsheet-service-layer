with all_districts as (
select 
  d.district_id,
  d.funding_year,
  d.num_students,
  d.num_schools,
  dfa.fit_for_ia,
  dbc.meeting_2014_goal_no_oversub,
  sum(df.known_unscalable_campuses + df.assumed_unscalable_campuses) as num_schools_wo_fiber
  
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

where d.funding_year = 2019
and d.in_universe = true
and d.district_type='Traditional'

group by 1,2,3,4,5,6

union

select 
  d.district_id,
  d.funding_year,
  d.num_students,
  d.num_schools,
  dfa.fit_for_ia,
  dbc.meeting_2014_goal_no_oversub,
  sum(df.known_unscalable_campuses + df.assumed_unscalable_campuses) as num_schools_wo_fiber
  
from ps.districts_frozen_sots d

join ps.districts_bw_cost_frozen_sots dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_fiber_frozen_sots df
on d.district_id= df.district_id
and d.funding_year = df.funding_year

join ps.districts_fit_for_analysis_frozen_sots dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

where d.funding_year = 2018
--and d.in_universe=true
and d.district_type='Traditional'

group by 1,2,3,4,5,6
),

connectivity_agg as (
select funding_year,

(sum(case when meeting_2014_goal_no_oversub = false and fit_for_ia = true then num_students else 0 end)::numeric/
  sum(case when fit_for_ia = true then num_students else 0 end))*sum(num_students)
as num_students_not_meeting 

from all_districts
group by 1),

fiber_agg as (
select funding_year,
sum(num_schools_wo_fiber) as num_schools_wo_fiber
from all_districts
group by 1)

select 'connectivity' as metric,

round(c17.num_students_not_meeting -
c18.num_students_not_meeting, -5)
as num

from (select * from connectivity_agg where funding_year = 2018) c17
join (select * from connectivity_agg where funding_year = 2019) c18
on true

union

select 'fiber' as metric,

(f18.num_schools_wo_fiber - f17.num_schools_wo_fiber)::numeric/
f17.num_schools_wo_fiber
as num

from (select * from fiber_agg where funding_year = 2018) f17
join (select * from fiber_agg where funding_year = 2019) f18
on true