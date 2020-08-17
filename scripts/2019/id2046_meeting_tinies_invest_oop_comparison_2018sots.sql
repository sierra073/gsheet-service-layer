with districts as (
	select d.district_id,
	d.size,
	d.num_students,
	bw.meeting_2018_goal_oversub,
	bw.ia_annual_cost_total - bw.ia_funding_requested_erate 
	as ia_annual_oop,
	(bw.ia_annual_cost_total - bw.ia_funding_requested_erate)/d.num_students 
	as ia_annual_oop_student


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
	and fit.fit_for_ia_cost = true
	and bw.ia_annual_cost_total > 0 

),

calcs as (select 
	sum(ia_annual_oop)/sum(num_students) as weighted_avg_ia_annual_oop_student_all,

	sum(case 
			when size = 'Tiny' and meeting_2018_goal_oversub = true 
				then ia_annual_oop end)/
		sum(case 
				when size = 'Tiny' and meeting_2018_goal_oversub = true 
					then num_students end) as weighted_avg_ia_annual_oop_student_meeting_tiny

	from districts
)

select 
round(weighted_avg_ia_annual_oop_student_all,2) as weighted_avg_ia_annual_oop_student_all,
round(weighted_avg_ia_annual_oop_student_meeting_tiny,2) as weighted_avg_ia_annual_oop_student_meeting_tiny,
round(weighted_avg_ia_annual_oop_student_meeting_tiny/weighted_avg_ia_annual_oop_student_all,2) as invest_x_times_as_much

from calcs
