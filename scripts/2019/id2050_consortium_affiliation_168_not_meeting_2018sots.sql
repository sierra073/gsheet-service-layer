select 
	d.district_id,
	d.name,
	d.state_code,
	d.size,
	d.locale,
	d.num_students,
	d.consortium_affiliation is not null as consortium_affiliation_boolean,
	d.consortium_affiliation,
	ss.procurement as state_procurement,
	ss.org_structure as state_org_structure

	from ps.districts_fit_for_analysis fit 

	inner join ps.districts d
	on d.district_id = fit.district_id
	and d.funding_year = fit.funding_year

	inner join ps.districts_bw_cost bw 
	on bw.district_id = fit.district_id
	and bw.funding_year = fit.funding_year

	inner join  ps.states_static ss 
	on d.state_code = ss.state_code


	where d.funding_year = 2019
	and d.district_type = 'Traditional'
	and d.in_universe = true 
	and bw.meeting_2014_goal_no_oversub = false
	and fit.fit_for_ia = true 

	and d.num_students < 9000