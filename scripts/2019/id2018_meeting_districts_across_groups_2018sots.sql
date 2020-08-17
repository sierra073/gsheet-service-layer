with all_yrs as (select 
	dd.district_id, 
	dd.funding_year,
	dd.size,
	dd.locale,
	dd.state_code,
	dd.num_students,
	fit.fit_for_ia,
	fit.fit_for_ia_cost,
	CASE 
		WHEN fit.fit_for_ia = TRUE
			THEN bc.ia_bw_mbps_total
	END as ia_bw_mbps_total, 
	CASE 
		WHEN fit.fit_for_ia = TRUE
			THEN bc.meeting_2014_goal_no_oversub
	END as meeting_2014_goal_no_oversub,
	CASE 
		WHEN fit.fit_for_ia = TRUE
			THEN bc.meeting_2018_goal_oversub
	END as meeting_2018_goal_oversub,
	CASE 
		WHEN fit.fit_for_ia = TRUE
			THEN bc.ia_bandwidth_per_student_kbps
	END as ia_bandwidth_per_student_kbps,
	CASE 
		WHEN fit.fit_for_ia_cost = TRUE 
			THEN bc.ia_monthly_cost_total
	END as ia_monthly_cost_total,
	CASE 
		WHEN fit.fit_for_ia_cost = TRUE 
			THEN bc.ia_monthly_cost_per_mbps
	END as ia_monthly_cost_per_mbps

 	FROM ps.districts dd 

 	JOIN ps.districts_fit_for_analysis fit
 	ON fit.district_id = dd.district_id
 	AND fit.funding_year = dd.funding_year

 	JOIN ps.districts_bw_cost bc
	ON dd.district_id = bc.district_id
	AND dd.funding_year = bc.funding_year



 	WHERE dd.in_universe = true
 	AND dd.district_type = 'Traditional'


 	),

year_total as (select funding_year,
'Total'::varchar as district_group,
count(case 
	when meeting_2018_goal_oversub = TRUE
		then district_id end) as districts_meeting,
count(case 
	when meeting_2018_goal_oversub = TRUE
		then district_id end)::numeric/
		count(case 
			when meeting_2018_goal_oversub is not null
				then district_id end) as percent_meeting_of_total_pop,
count(district_id) as districts_pop,
1 as percent_of_meeting_districts

from all_yrs 

group by funding_year
),

counts_and_percents as (
select s.funding_year,
s.size as district_group,
count(case 
	when meeting_2018_goal_oversub = TRUE
		then district_id end) as districts_meeting,
count(case 
	when meeting_2018_goal_oversub = TRUE
		then district_id end)::numeric/
		count(case 
			when meeting_2018_goal_oversub is not null
				then district_id end) as percent_meeting_of_total_pop,
count(district_id) as districts_pop,
count(case 
	when meeting_2018_goal_oversub = TRUE
		then district_id end)::numeric/t.districts_meeting as percent_of_meeting_districts,
1 as order_group 


from all_yrs s

left join year_total t 
on t.funding_year = s.funding_year

group by s.funding_year,
s.size,
t.districts_meeting


union 

select s.funding_year,
case 
	when s.size in ('Tiny','Small')
		then 'Tiny & Small'
	when s.size in ('Medium','Large','Mega')
		then 'Medium, Large & Mega' 
end as district_group,
count(case 
	when meeting_2018_goal_oversub = TRUE
		then district_id end) as districts_meeting,
count(case 
	when meeting_2018_goal_oversub = TRUE
		then district_id end)::numeric/
		count(case 
			when meeting_2018_goal_oversub is not null
				then district_id end) as percent_meeting_of_total_pop,
count(district_id) as districts_pop,
count(case 
	when meeting_2018_goal_oversub = TRUE
		then district_id end)::numeric/t.districts_meeting as percent_of_meeting_districts,
2 as order_group


from all_yrs s

left join year_total t 
on t.funding_year = s.funding_year

group by 1,2,t.districts_meeting

union 

select *,
3 as order_group
from year_total

),

median_metrics as (	select s.funding_year,
	s.size as district_group,
	median(ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps,
	median(ia_monthly_cost_total) as median_ia_monthly_cost_total,
	median(ia_monthly_cost_per_mbps) as median_ia_monthly_cost_per_mbps,
	median(ia_bw_mbps_total) as median_ia_bw_mbps_total

	from all_yrs s

	where meeting_2018_goal_oversub = TRUE

	group by s.funding_year,
	s.size

union

	select s.funding_year,
	case 
		when s.size in ('Tiny','Small')
			then 'Tiny & Small'
		when s.size in ('Medium','Large','Mega')
			then 'Medium, Large & Mega' 
	end as district_group,
	median(ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps,
	median(ia_monthly_cost_total) as median_ia_monthly_cost_total,
	median(ia_monthly_cost_per_mbps) as median_ia_monthly_cost_per_mbps,
	median(ia_bw_mbps_total) as median_ia_bw_mbps_total

	from all_yrs s

	where meeting_2018_goal_oversub = TRUE

	group by 1,2


union

	select funding_year,
	'Total'::varchar as district_group,
	median(ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps,
	median(ia_monthly_cost_total) as median_ia_monthly_cost_total,
	median(ia_monthly_cost_per_mbps) as median_ia_monthly_cost_per_mbps,
	median(ia_bw_mbps_total) as median_ia_bw_mbps_total

	from all_yrs 

	where meeting_2018_goal_oversub = TRUE

	group by funding_year
)


select cp.funding_year as funding_year,
cp.district_group,
cp.districts_meeting,
cp.percent_meeting_of_total_pop,
cp.districts_pop,
cp.percent_of_meeting_districts,
mm.median_ia_bandwidth_per_student_kbps,
mm.median_ia_monthly_cost_total,
mm.median_ia_monthly_cost_per_mbps,
mm.median_ia_bw_mbps_total

from counts_and_percents cp

join median_metrics mm
on cp.funding_year = mm.funding_year
and cp.district_group = mm.district_group 



order by cp.order_group asc, cp.district_group asc, cp.funding_year desc
