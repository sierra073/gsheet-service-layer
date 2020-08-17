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
	w.remaining_post/d.num_students as remaining_per_student

	from ps.districts_fit_for_analysis fit 

	inner join ps.districts d
	on d.district_id = fit.district_id
	and d.funding_year = fit.funding_year

	inner join ps.districts_wifi w
	on w.district_id = fit.district_id 
	and w.funding_year = fit.funding_year

	where d.district_type = 'Traditional'
	and d.in_universe = true 
	and (w.remaining_post / w.budget_post) >= .5
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
remaining_per_student


from districts
where funding_year = 2019

)

select count(district_id) as districts_funding_at_risk,
sum(remaining_post)/sum(num_students) as weighted_avg_funding_remaining_per_student,
median(remaining_per_student) as median_funding_remaining_per_student,
sum(budget_post)/sum(num_students) as weighted_avg_budget_per_student,
median(budget_post) as median_budget_per_student


from districts_2

where district_2014er = true 