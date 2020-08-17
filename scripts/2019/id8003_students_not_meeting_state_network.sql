select d.state_code, sum(d.num_students) as students, count(*) as districts
from ps.districts d
join ps.districts_bw_cost db
on d.district_id = db.district_id
and d.funding_year = db.funding_year
join ps.districts_fit_for_analysis df
on d.district_id = df.district_id
and d.funding_year = df.funding_year
join ps.states_static s 
on s.state_code = d.state_code
where d.funding_year = 2019
and db.meeting_2014_goal_no_oversub = false
and d.in_universe = true
and d.district_type = 'Traditional'
and df.fit_for_ia = true
and s.state_network_natl_analysis = true
group by 1