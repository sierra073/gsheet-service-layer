with districts as (
		select d.state_code,
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

		inner join ps.districts_bw_cost bw 
		on bw.district_id = fit.district_id
		and bw.funding_year = fit.funding_year

		inner join ps.districts_fiber f 
		on f.district_id = fit.district_id
		and f.funding_year = fit.funding_year

		where d.district_type = 'Traditional'
		and d.in_universe = true 
		and d.funding_year = 2019

)

select 
	funding_year,
	
	round(1- (sum(unscalable_campuses)/sum(num_campuses)),2)
	as fiber_campuses,

	--connectivity

	round(case 
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
	end,2) as connectivity_schools


	from districts

	group by funding_year

