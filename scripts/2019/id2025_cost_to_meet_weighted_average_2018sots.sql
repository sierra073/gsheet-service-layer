with meeting_cost as (
	select sum(bc.ia_monthly_cost_total)/sum(bc.ia_bw_mbps_total) as weighted_avg_cost_per_mbps,
	median(bc.ia_monthly_cost_per_mbps) as median_cost_per_mbps

	FROM ps.districts dd 

	JOIN ps.districts_fit_for_analysis fit
	ON fit.district_id = dd.district_id
	AND fit.funding_year = dd.funding_year

	JOIN ps.districts_bw_cost bc
	ON dd.district_id = bc.district_id
	AND dd.funding_year = bc.funding_year

	WHERE dd.funding_year = 2019
	and dd.in_universe = true
	and dd.district_type = 'Traditional'
	and fit.fit_for_ia = true
	and fit.fit_for_ia_cost = true 
	and bc.meeting_2018_goal_oversub = true 
),

not_meeting as (
	select dd.district_id,
	dd.num_students,
	bc.projected_bw_fy2019_cck12, 
	--(dd.num_students::numeric * dd.setda_concurrency_factor) as projected_bw_fy2019_no_rounding,
	bc.ia_monthly_cost_total,
	bc.projected_bw_fy2019_cck12 * meeting_cost.weighted_avg_cost_per_mbps as cck12_bw_weighted_average_cost,
	bc.projected_bw_fy2019_cck12 * meeting_cost.median_cost_per_mbps as cck12_bw_median_cost,
	--(dd.num_students::numeric * dd.setda_concurrency_factor) * meeting_cost.weighted_avg_cost_per_mbps as no_rounding_bw_weighted_average_cost,
	--(dd.num_students::numeric * dd.setda_concurrency_factor) * meeting_cost.median_cost_per_mbps as no_rounding_bw_median_cost,
	meeting_cost.weighted_avg_cost_per_mbps,
	meeting_cost.median_cost_per_mbps


	FROM ps.districts dd 

	JOIN ps.districts_fit_for_analysis fit
	ON fit.district_id = dd.district_id
	AND fit.funding_year = dd.funding_year

	JOIN ps.districts_bw_cost bc
	ON dd.district_id = bc.district_id
	AND dd.funding_year = bc.funding_year

	JOIN meeting_cost 
	on TRUE 

	WHERE dd.funding_year = 2019
	and dd.in_universe = true
	and dd.district_type = 'Traditional'
	and fit.fit_for_ia = true
	and fit.fit_for_ia_cost = true 
	and bc.meeting_2018_goal_oversub = false 

)

select 
weighted_avg_cost_per_mbps,
sum(cck12_bw_weighted_average_cost) as total_weighted_average_cost,
count(case 
	when ia_monthly_cost_total >= cck12_bw_weighted_average_cost
		then district_id
end) as districts_can_meet_weighted_average_cost,
count(case 
	when ia_monthly_cost_total >= cck12_bw_weighted_average_cost
		then district_id
end)/count(district_id)::numeric as districts_can_meet_weighted_average_cost_p,
sum(case 
	when ia_monthly_cost_total >= cck12_bw_weighted_average_cost
		then num_students
end)/sum(num_students)::numeric as students_can_meet_weighted_average_cost_p,


median_cost_per_mbps,
sum(cck12_bw_median_cost) as total_median_cost,
count(case 
	when ia_monthly_cost_total >= cck12_bw_median_cost
		then district_id
end) as districts_can_meet_median_cost,
count(case 
	when ia_monthly_cost_total >= cck12_bw_median_cost
		then district_id
end)/count(district_id)::numeric as districts_can_meet_median_cost_p,
sum(case 
	when ia_monthly_cost_total >= cck12_bw_median_cost
		then num_students
end)/sum(num_students)::numeric as students_can_meet_median_cost_p


from not_meeting 

group by weighted_avg_cost_per_mbps,
median_cost_per_mbps


