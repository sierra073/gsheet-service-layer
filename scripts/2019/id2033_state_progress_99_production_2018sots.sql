with districts as (select d.state_code,
	d.funding_year,
	d.district_id,
	d.num_students,
	d.num_campuses,
	d.num_schools,
	f.fiber_target_status,
	case 
		when fit.fit_for_ia_cost = true and bw.ia_bw_mbps_total > 0
			then bw.ia_bw_mbps_total
	end as ia_bw_mbps_total,
	case 
		when fit.fit_for_ia_cost = true and bw.ia_monthly_cost_total > 0
			then bw.ia_monthly_cost_total  
	end as ia_monthly_cost_total,

	--fiber
	f.known_unscalable_campuses + f.assumed_unscalable_campuses
	as unscalable_campuses,

	-- connectivity 
	case 
		when fit.fit_for_ia = true
			then bw.meeting_2014_goal_no_oversub 
	end as meeting_2014_goal_no_oversub

	from ps.districts_fit_for_analysis fit 

	inner join ps.districts d
	on d.district_id = fit.district_id
	and d.funding_year = fit.funding_year

	inner join ps.districts_wifi w
	on w.district_id = fit.district_id 
	and w.funding_year = fit.funding_year

	inner join ps.districts_bw_cost bw 
	on bw.district_id = fit.district_id
	and bw.funding_year = fit.funding_year

	inner join ps.districts_fiber f 
	on f.district_id = fit.district_id
	and f.funding_year = fit.funding_year

	where d.district_type = 'Traditional'
	and d.in_universe = true 
	and d.state_code != 'DC'

),

states_pre as (select 
	funding_year,
	state_code,
	--affordability
	sum(ia_monthly_cost_total)/sum(ia_bw_mbps_total) as weighted_avg_$_mbps,
	--fiber
	case 
		when count(case 
				when fiber_target_status in ('Target','Not Target')
					then district_id
					end) = 0
			then 0
		else count(case 
			when fiber_target_status = 'Not Target'
				then district_id
			end)::numeric/count(case 
				when fiber_target_status in ('Target','Not Target')
					then district_id
					end)
	end as fiber_districts,
	1- (sum(unscalable_campuses)/sum(num_campuses))
	as fiber_campuses,

	--connectivity
	case 
		when count(case 
					when meeting_2014_goal_no_oversub is not null 
						then district_id
					end) = 0
			then 0
		else  
				count(case 
					when meeting_2014_goal_no_oversub = true 
						then district_id
					end)::numeric/
							count(case 
								when meeting_2014_goal_no_oversub is not null 
									then district_id
								end)
	end as connectivity_districts,
	case 
		when sum(case 
					when meeting_2014_goal_no_oversub is not null 
						then num_schools
					end) = 0
			then 0
		else  
				sum(case 
					when meeting_2014_goal_no_oversub = true 
						then num_schools
					end)::numeric/
							sum(case 
								when meeting_2014_goal_no_oversub is not null 
									then num_schools
								end)
	end as connectivity_schools,
	case 
		when sum(case 
					when meeting_2014_goal_no_oversub is not null 
						then num_students
					end) = 0
			then 0
		else  
				sum(case 
					when meeting_2014_goal_no_oversub = true 
						then num_students
					end)::numeric/
							sum(case 
								when meeting_2014_goal_no_oversub is not null 
									then num_students
								end)
	end as connectivity_students


	from districts

	group by funding_year,
	state_code

),

states as (select funding_year,
	state_code,
	round(weighted_avg_$_mbps,2) as weighted_avg_$_mbps,
	round(fiber_districts,2) as fiber_districts,
	round(fiber_campuses,2) as fiber_campuses,
	round(connectivity_districts,2) as connectivity_districts,
	round(connectivity_schools,2) as connectivity_schools,
	round(connectivity_students,2) as connectivity_students

	from states_pre

)

	select funding_year,

	count(case 
		when weighted_avg_$_mbps <= 3 
			then state_code
		end)
	as states_at_$3_mbps,

	count(case
		when fiber_districts >= .99
			then state_code
		end)
	as state_fiber_districts,
	count(case
		when fiber_campuses >= .99
			then state_code
		end)
	as state_fiber_campuses,

	count(case
		when connectivity_districts >= .99
			then state_code
		end)
	as state_connectivity_districts,
	count(case
		when connectivity_schools >= .99
			then state_code
		end)
	as state_connectivity_schools,
	count(case
		when connectivity_students >= .99
			then state_code
		end)
	as state_connectivity_students,
	count(case 
		when connectivity_districts >= .99 and fiber_districts >= .99
			then state_code
		end)
	as state_fiber_and_connectivity_districts,
	count(case 
		when connectivity_schools >= .99 and fiber_campuses >= .99
			then state_code
		end)
	as state_fiber_and_connectivity_schools

	from states

	group by funding_year

	order by funding_year

	
