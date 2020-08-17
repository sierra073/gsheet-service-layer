with districts as (select d.state_code,
	d.funding_year,
	d.district_id,
	d.num_students,
	d.num_campuses,

	--wifi
	w.remaining_post / w.budget_post as wifi_perc_remaining_postdiscount,

	--fiber
	case 
		when fw.funding_year = 2019
			then fw.known_unscalable_campuses_fine_wine + fw.assumed_unscalable_campuses_fine_wine 
		else fw.known_unscalable_campuses + fw.assumed_unscalable_campuses
	end as unscalable_campuses,

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

	inner join ps.smd_2019_fine_wine fw
	on fw.district_id = fit.district_id
	and fw.funding_year = fit.funding_year

	inner join ps.districts_bw_cost bw 
	on bw.district_id = fit.district_id
	and bw.funding_year = fit.funding_year

	where d.district_type = 'Traditional'
	and d.in_universe = true 
	and d.state_code != 'DC'

),

states as (select 
	funding_year,
	state_code,
	--wifi
	count(case 
		when wifi_perc_remaining_postdiscount < .5
			then district_id
		end)::numeric/count(district_id) 
	as districts_that_used_50percent_or_more_of_c2_funds_p,

	--fiber
	1- (sum(unscalable_campuses)/sum(num_campuses))
	as campuses_scalable_p,

	--connectivity
	case 
		when sum(case 
					when meeting_2014_goal_no_oversub is not null 
						then num_students
					else 0
					end) = 0
			then 0
		else  
				sum(case 
					when meeting_2014_goal_no_oversub = true 
						then num_students
					else 0
					end)::numeric/
							sum(case 
								when meeting_2014_goal_no_oversub is not null 
									then num_students
								else 0 
								end)
	end as students_connected_p


	from districts

	group by funding_year,
	state_code

)

	select funding_year,
	.99 as threshold,
	count(case 
			when districts_that_used_50percent_or_more_of_c2_funds_p >= .99
				then state_code
		end)
	as state_wifi,
	count(case
		when campuses_scalable_p >= .99
			then state_code
		end)
	as state_fiber,
	count(case
		when students_connected_p >= .99
			then state_code
		end)
	as state_connectivity

	from states

	group by funding_year

union 

select funding_year,
	.95 as threshold,
	count(case 
			when districts_that_used_50percent_or_more_of_c2_funds_p >= .95
				then state_code
		end)
	as state_wifi,
	count(case
		when campuses_scalable_p >= .95
			then state_code
		end)
	as state_fiber,
	count(case
		when students_connected_p >= .95
			then state_code
		end)
	as state_connectivity

	from states

	group by funding_year

union 

	select funding_year,
	.90 as threshold,
	count(case 
			when districts_that_used_50percent_or_more_of_c2_funds_p >= .90
				then state_code
		end)
	as state_wifi,
	count(case
		when campuses_scalable_p >= .90
			then state_code
		end)
	as state_fiber,
	count(case
		when students_connected_p >= .90
			then state_code
		end)
	as state_connectivity

	from states

	group by funding_year

union 

		select funding_year,
	.65 as threshold,
	count(case 
			when districts_that_used_50percent_or_more_of_c2_funds_p >= .65
				then state_code
		end)
	as state_wifi,
	count(case
		when campuses_scalable_p >= .65
			then state_code
		end)
	as state_fiber,
	count(case
		when students_connected_p >= .65
			then state_code
		end)
	as state_connectivity

	from states

	group by funding_year


order by threshold,funding_year
