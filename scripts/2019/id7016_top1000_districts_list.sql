select 
d.district_id,
d.name,
d.state_code,
d.num_students,
fit.fit_for_ia,
case
	when fit.fit_for_ia = true 
		then bw.meeting_2018_goal_oversub
end as  meeting_2018_goal_oversub,
case
	when fit.fit_for_ia = true 
		then bw.ia_bandwidth_per_student_kbps
end as ia_bandwidth_per_student_kbps

from ps.districts_fit_for_analysis fit 

inner join ps.districts d
on d.district_id = fit.district_id
and d.funding_year = fit.funding_year

inner join ps.districts_bw_cost bw 
on bw.district_id = fit.district_id
and bw.funding_year = fit.funding_year

where d.funding_year = 2019
and d.district_type = 'Traditional'
and d.in_universe = true 

order by d.num_students desc

limit 1000
