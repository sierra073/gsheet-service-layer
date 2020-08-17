with tiny_d as (
	select d.district_id,
	d.num_students,
	d.locale,
	f.hierarchy_ia_connect_category = 'Fiber' as fiber_ia,
	f.fiber_target_status = 'Target' as fiber_target

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

round(count(case
	when locale = 'Rural' 
		then district_id end)::numeric/count(district_id),2)
as districts_rural_p,

round(count(case 
	when fiber_ia = true 
		then district_id end)::numeric/count(district_id),2)
as districts_fiber_p,

round(count(case
	when fiber_ia = true or fiber_target = false 
		then district_id end)::numeric/count(district_id),2)
as districts_fiber_or_other_scalable_p

from tiny_d