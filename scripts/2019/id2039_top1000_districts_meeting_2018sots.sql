with top_1k_students as (
	select 
	d.district_id,
	fit.fit_for_ia,
	bw.meeting_2018_goal_oversub,
	d.num_students

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
	)

select
count(case
	when fit_for_ia = true and meeting_2018_goal_oversub = true 
		then district_id end)::numeric/
	count(case
		when fit_for_ia = true
			then district_id end)
as percent_meeting_1mbps,
count(case
	when fit_for_ia = true and meeting_2018_goal_oversub = true 
		then district_id end)::numeric
as number_meeting_1mpbs,
count(case
	when fit_for_ia = true 
		then district_id end)::numeric/
	count(district_id)
as percent_clean,
count(case
	when fit_for_ia = true 
		then district_id end)
as number_clean

from top_1k_students
