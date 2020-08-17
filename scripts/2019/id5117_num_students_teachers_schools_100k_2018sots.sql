with all_districts as (
select 
  d.district_id,
  d.funding_year,
  d.num_students,
  d.num_schools,
  d.num_teachers,
  dfa.fit_for_ia,
  dbc.meeting_2014_goal_no_oversub
  
from ps.districts d

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

where d.funding_year = 2019
and d.in_universe = true
and d.district_type='Traditional'),

sample as (
select meeting_2014_goal_no_oversub,
sum(num_students) as num_students,
sum(num_teachers) as num_teachers,
sum(num_schools) as num_schools
from all_districts
where fit_for_ia = true
group by 1)

select 
round((sample.num_students::numeric/sample_pop.num_students) * pop.num_students, -5) as num_students,
round((sample.num_teachers::numeric/sample_pop.num_teachers) * pop.num_teachers, -5) as num_teachers,
round((sample.num_schools::numeric/sample_pop.num_schools) * pop.num_schools, 0) as num_schools,
sample.num_schools::numeric/sample_pop.num_schools as pct_schools

from 
(select * from sample where meeting_2014_goal_no_oversub = true) sample

join (
select sum(num_students) as num_students,
sum(num_teachers) as num_teachers,
sum(num_schools) as num_schools
from sample) sample_pop
on true

join (
select sum(num_students) as num_students,
sum(num_teachers) as num_teachers,
sum(num_schools) as num_schools
from all_districts) pop
on true