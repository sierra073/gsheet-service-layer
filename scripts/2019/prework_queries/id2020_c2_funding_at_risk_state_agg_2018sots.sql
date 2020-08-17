with districts as (select d.state_code,
	d.funding_year,
	d.district_id,
	d.num_students,
	d.num_campuses,
	d.latitude,
	d.longitude,

	-- metrics
	w.remaining_post / w.budget_post as perc_remaining_postdiscount,
	(w.remaining_post / w.budget_post) = 1 as district_100percenters,
	min(case 
			when w.budget_post > w.remaining_post
				then w.funding_year
			else null end) over (partition by fit.district_id)
	as year_started,
	(min(case 
			when w.budget_post > w.remaining_post
				then w.funding_year
			else null end) over (partition by fit.district_id)
		= 2015)
	as district_2014er,
	w.remaining_post,
	w.budget_post,
	w.remaining_post/d.num_students as remaining_per_student,
	(w.remaining_post / w.budget_post) >= .5 as more_than_50_remaining

	from ps.districts_fit_for_analysis fit 

	inner join ps.districts d
	on d.district_id = fit.district_id
	and d.funding_year = fit.funding_year

	inner join ps.districts_wifi w
	on w.district_id = fit.district_id 
	and w.funding_year = fit.funding_year

	where d.district_type = 'Traditional'
	and d.in_universe = true 
	and d.state_code != 'DC'

),

districts_2 as (select state_code,
	funding_year,
	district_id,
	num_students,
	num_campuses,
	latitude,
	longitude,
	perc_remaining_postdiscount,
	case 
		/* it looks like there are 6 districts that have started their clocks 
		but because their student count and therefore budget has gone down it looks like they haven't used any funds*/
		when year_started is not null
			then false
		else district_100percenters
	end as district_100percenters,
	year_started,
	case 
		when district_2014er is null
			then false
		else district_2014er
	end as district_2014er,
	remaining_post,
	budget_post,
	remaining_per_student,
	more_than_50_remaining


	from districts
	where funding_year = 2019

)


select state_code,
sum(case 
	when more_than_50_remaining = TRUE and (district_100percenters = true or district_2014er = true)
		then remaining_post
	else 0
	end)/sum(budget_post)
as percent_risk_of_original_budget,
sum(budget_post) as total_original_budget,
sum(case 
	when more_than_50_remaining = TRUE and (district_100percenters = true or district_2014er = true)
		then remaining_post
	else 0
	end) as funding_at_risk

from districts_2

group by state_code