with all_yrs as (
	select 
	dd.funding_year,
	dd.district_id,
	dd.state_code,
	dd.name,
	dd.num_students,
	dd.locale,
	dd.size,
	CASE 
		WHEN fit.fit_for_ia = TRUE
			THEN bc.meeting_2018_goal_oversub
	END as meeting_2018_goal_oversub,
	CASE 
		WHEN fit.fit_for_ia = TRUE 
			THEN bc.ia_bw_mbps_total
	END as ia_bw_mbps_total,
	CASE 
		WHEN fit.fit_for_ia = true 
			THEN round(bc.ia_bandwidth_per_student_kbps)
	END as ia_bandwidth_per_student_kbps,
	f.hierarchy_ia_connect_category

	FROM ps.districts dd 

	JOIN ps.districts_fit_for_analysis fit
	ON fit.district_id = dd.district_id
	AND fit.funding_year = dd.funding_year

	JOIN ps.districts_bw_cost bc
	ON dd.district_id = bc.district_id
	AND dd.funding_year = bc.funding_year

	JOIN  ps.districts_fiber  f 
	ON dd.district_id = f.district_id
	AND dd.funding_year = f.funding_year

	WHERE dd.in_universe = true
	AND dd.district_type = 'Traditional'
)

select fy18.district_id,
fy18.state_code,
fy18.name,
fy18.num_students,
fy18.locale,
fy18.size,
fy18.meeting_2018_goal_oversub,
fy18.ia_bw_mbps_total as fy18_ia_bw_mbps_total,
fy17.ia_bw_mbps_total as fy17_ia_bw_mbps_total,
fy18.ia_bandwidth_per_student_kbps as fy18_ia_bandwidth_per_student_kbps,
fy17.ia_bandwidth_per_student_kbps as fy17_ia_bandwidth_per_student_kbps,
fy18.hierarchy_ia_connect_category as fy18_hierarchy_ia_connect_category,
fy17.hierarchy_ia_connect_category as fy17_hierarchy_ia_connect_category


from all_yrs fy18

left join all_yrs fy17 
on fy18.district_id = fy17.district_id
and fy17.funding_year = 2018

where fy18.funding_year = 2019 
and fy18.meeting_2018_goal_oversub = true 
