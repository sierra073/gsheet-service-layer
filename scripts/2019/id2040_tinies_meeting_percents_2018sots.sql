with tiny_d as (
	select d.district_id,
	d.num_students,
	d.locale,
	f.hierarchy_ia_connect_category = 'Fiber' as fiber_ia,
	f.fiber_target_status = 'Target' as fiber_target,
	bw.ia_bw_mbps_total,
	bw.ia_bandwidth_per_student_kbps,
	case 
		when bw.ia_annual_cost_total > 0 and fit.fit_for_ia_cost = true
			then bw.ia_annual_cost_total - bw.ia_funding_requested_erate 
	end as ia_annual_oop,
	case
		when bw.ia_annual_cost_total > 0 and fit.fit_for_ia_cost = true
			then (bw.ia_annual_cost_total - bw.ia_funding_requested_erate)/d.num_students 
	end as ia_annual_oop_student


	from ps.districts_fit_for_analysis fit 

	inner join ps.districts d
	on d.district_id = fit.district_id
	and d.funding_year = fit.funding_year

	inner join ps.districts_bw_cost bw 
	on bw.district_id = fit.district_id
	and bw.funding_year = fit.funding_year

	inner join ps.districts_fiber f
	on f.district_id = fit.district_id
	and f.funding_year = fit.funding_year

	where d.funding_year = 2019
	and d.district_type = 'Traditional'
	and d.in_universe = true 
	and fit.fit_for_ia = true
	and bw.meeting_2018_goal_oversub = true 
	and d.size = 'Tiny'
)

select count(district_id) as districts,

count(case
	when locale = 'Rural' 
		then district_id end)::numeric/count(district_id)
as districts_rural_p,
count(case
	when locale = 'Town' 
		then district_id end)::numeric/count(district_id)
as districts_town_p,
count(case
	when locale in ('Town','Rural') 
		then district_id end)::numeric/count(district_id)
as districts_rural_or_town_p,

count(case 
	when fiber_ia = true 
		then district_id end)::numeric/count(district_id)
as districts_fiber_p,
count(case 
	when fiber_ia = false and fiber_target = true 
		then district_id end)::numeric/
	count(case
			when fiber_ia = false
				then district_id end)
as districts_non_fiber_fiber_target,

median(ia_bandwidth_per_student_kbps) as median_bw_student,
sum(ia_bw_mbps_total)*1000/sum(num_students) as weighted_avg_bw_student,

median(ia_annual_oop_student) as median_ia_annual_oop_student,
sum(ia_annual_oop)/sum(case 
		when ia_annual_oop is not null 
			then num_students end) as weighted_avg_ia_annual_oop_student

from tiny_d