with c2_classrooms as 
(select 
d.state_code,
d.district_id,
1-(dw.remaining_post/dw.budget_post)::float as percent_classrooms_upgraded,

-- assume 1 teacher/classroom
-- d.num_teachers as num_classrooms,
-- (1-(dw.remaining_post/dw.budget_post)::float) * d.num_teachers as num_classrooms_upgraded

-- assume 1.5 teacher/classroom
-- d.num_teachers/1.5 as num_classrooms,
-- (1-(dw.remaining_post/dw.budget_post)::float) * d.num_teachers/1.5 as num_classrooms_upgraded

-- assume 23.3 students/classroom on average : based on avg(students/teacher)
d.num_students / 23.3 as num_classrooms,
(1-(dw.remaining_post/dw.budget_post)::float) * d.num_students / 23.3 as num_classrooms_upgraded

from ps.districts d

join ps.districts_wifi dw
on d.district_id = dw.district_id
and d.funding_year = dw.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and d.funding_year = 2019)

select round(sum(num_classrooms_upgraded)) as num_classrooms_wifi_upgraded
from c2_classrooms


