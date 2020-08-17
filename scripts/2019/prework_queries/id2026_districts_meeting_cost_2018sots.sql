	select 
	case 
		when bc.meeting_2018_goal_oversub = true
			then 'Meeting 1 Mbps/student'
		when bc.meeting_2014_goal_no_oversub = true 
			then 'Meeting 100 kbps/student, not 1 Mbps'
		when bc.meeting_2014_goal_no_oversub = false 
			then 'Not Meeting 100 kbps/student'
	end as meeting_goal_group,
	sum(bc.ia_monthly_cost_total)/sum(bc.ia_bw_mbps_total) as weighted_avg_cost_per_mbps,
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

	group by 1