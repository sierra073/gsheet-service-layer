with districts as (
	select d.district_id,
	d.num_students,
	bw.meeting_2018_goal_oversub,
	case 
		when bw.ia_annual_cost_total > 0 
			then bw.ia_annual_cost_total - bw.ia_funding_requested_erate 
	end as ia_annual_oop,
	case
		when bw.ia_annual_cost_total > 0 
			then (bw.ia_annual_cost_total - bw.ia_funding_requested_erate)/d.num_students 
	end as ia_annual_oop_student


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

)

select 'All' as group,
median(ia_annual_oop_student) as median_ia_annual_oop_student,
sum(ia_annual_oop)/sum(case 
		when ia_annual_oop is not null 
			then num_students end) as weighted_avg_ia_annual_oop_student

from districts

union 

select 
case 
	when meeting_2018_goal_oversub = true 
		then 'Meeting 2019'
	when meeting_2018_goal_oversub = false 
		then 'Not Meeting 2019'
end as group,
median(ia_annual_oop_student) as median_ia_annual_oop_student,
sum(ia_annual_oop)/sum(case 
		when ia_annual_oop is not null 
			then num_students end) as weighted_avg_ia_annual_oop_student

from districts 

group by meeting_2018_goal_oversub