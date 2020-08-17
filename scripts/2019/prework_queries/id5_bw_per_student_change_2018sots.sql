select '2018' as year, meeting_2014_goal_no_oversub,
ia_bandwidth_per_student_kbps
from public.fy2018_districts_deluxe_matr 
where exclude_from_ia_analysis=false
and include_in_universe_of_districts
and district_type = 'Traditional'
union all
select '2017' as year, meeting_2014_goal_no_oversub,
ia_bandwidth_per_student_kbps
from public.fy2017_districts_deluxe_matr 
where exclude_from_ia_analysis=false
and include_in_universe_of_districts
and district_type = 'Traditional'
union all
select '2015' as year, meeting_2014_goal_no_oversub,
ia_bandwidth_per_student::numeric
from public.fy2015_districts_deluxe_m
where exclude_from_analysis=false
and ia_bandwidth_per_student not in ('Insufficient data','Infinity');