with districts as (
	select d.district_id,
	d.size,
	d.num_students,
	bw.meeting_2018_goal_oversub,
	bw.ia_bandwidth_per_student_kbps


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
	and fit.fit_for_ia = true
)


select round(median(ia_bandwidth_per_student_kbps),2) as bw_student_national_median,
round(median(ia_bandwidth_per_student_kbps) filter (where size = 'Tiny' and meeting_2018_goal_oversub = true),2) as bw_student_meeting_tiny,

round(median(ia_bandwidth_per_student_kbps) filter (where size = 'Tiny' and meeting_2018_goal_oversub = true)/median(ia_bandwidth_per_student_kbps),2) as bw_x_times_as_much

from districts

