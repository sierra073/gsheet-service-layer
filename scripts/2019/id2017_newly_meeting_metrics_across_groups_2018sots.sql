with all_yrs as (select 
	dd.district_id, 
	dd.funding_year,
	fit.fit_for_ia,
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
		WHEN fit.fit_for_ia_cost = TRUE AND dd.state_code != 'AK'
			THEN bc.ia_monthly_cost_total
	END as ia_monthly_cost_total,
	CASE 
		WHEN fit.fit_for_ia_cost = TRUE AND dd.state_code != 'AK'
			THEN bc.ia_monthly_cost_per_mbps
	END as ia_monthly_cost_per_mbps,
	dd.consortium_affiliation,
	sp.primary_sp,
	CASE 
		WHEN fit.fit_for_ia = TRUE and fit_older.fit_for_ia = TRUE
			THEN du.upgrade_indicator
	END as upgrade_indicator


 	FROM ps.districts dd 

 	JOIN ps.districts_fit_for_analysis fit
 	ON fit.district_id = dd.district_id
 	AND fit.funding_year = dd.funding_year

 	JOIN ps.districts_bw_cost bc
	ON dd.district_id = bc.district_id
	AND dd.funding_year = bc.funding_year

	JOIN ps.districts_sp_assignments sp
	on dd.district_id = sp.district_id
	and dd.funding_year = sp.funding_year

	JOIN ps.districts_upgrades du
	ON dd.district_id = du.district_id
 	AND dd.funding_year = du.funding_year

	LEFT JOIN  ps.districts_fit_for_analysis fit_older
 	ON fit_older.district_id = du.district_id
 	AND fit_older.funding_year = (du.funding_year -1)

 	WHERE dd.in_universe = true
 	AND dd.district_type = 'Traditional'

 	--AND fit.fit_for_ia = true
 	),

new_in_2019 as (select  
		dd.district_id,
		dd.name,
		dd.state_code,
		dd.locale,
		dd.size,
		dd.latitude,
		dd.longitude,
		dd.num_students,
		(dd.num_students::numeric * dd.setda_concurrency_factor) as projected_bw_fy2019_no_rounding,


		fy18.ia_bw_mbps_total as new_ia_bw_mbps_total,
		fy18.meeting_2014_goal_no_oversub as new_meeting_2014_goal_no_oversub,
		fy18.meeting_2018_goal_oversub as new_meeting_2018_goal_oversub,
		fy18.ia_bandwidth_per_student_kbps as new_ia_bandwidth_per_student_kbps,
		fy18.ia_monthly_cost_total as new_ia_monthly_cost_total,
		fy18.ia_monthly_cost_per_mbps as new_ia_monthly_cost_per_mbps,
		fy18.consortium_affiliation as new_consortium_affiliation,
		fy18.primary_sp as new_primary_sp,

		fy17.ia_bw_mbps_total as old_ia_bw_mbps_total,
		fy17.meeting_2014_goal_no_oversub as old_meeting_2014_goal_no_oversub,
		fy17.meeting_2018_goal_oversub as old_meeting_2018_goal_oversub,
		fy17.ia_bandwidth_per_student_kbps as old_ia_bandwidth_per_student_kbps,
		fy17.ia_monthly_cost_total as old_ia_monthly_cost_total,
		fy17.ia_monthly_cost_per_mbps as old_ia_monthly_cost_per_mbps,
		fy17.consortium_affiliation as old_consortium_affiliation,
		fy17.primary_sp as old_primary_sp,

		case 
			when fy17.ia_bw_mbps_total > 0 and fy18.ia_bw_mbps_total > 0
				then (fy18.ia_bw_mbps_total - fy17.ia_bw_mbps_total)
		end as meeting_2019_goal_upgrade_bw_increase,
		case 
			when fy17.ia_bw_mbps_total > 0 and fy18.ia_bw_mbps_total > 0
				then (fy18.ia_bw_mbps_total - fy17.ia_bw_mbps_total)/fy17.ia_bw_mbps_total 
		end AS meeting_2019_goal_upgrade_bw_increase_p,
		case 
			when fy17.ia_monthly_cost_total > 0 and fy18.ia_monthly_cost_total > 0
				then (fy18.ia_monthly_cost_total - fy17.ia_monthly_cost_total)
		end as meeting_2019_goal_upgrade_cost_increase,
		case 
			when fy17.ia_monthly_cost_total > 0 and fy18.ia_monthly_cost_total > 0
				then (fy18.ia_monthly_cost_total - fy17.ia_monthly_cost_total)/fy17.ia_monthly_cost_total 
		end as meeting_2019_goal_upgrade_cost_increase_p,
		(fy18.consortium_affiliation = fy17.consortium_affiliation) as same_consortia,
		(fy18.primary_sp = fy17.primary_sp) as same_primary_sp,
		case 
			when fy17.ia_monthly_cost_per_mbps > 0 and fy18.ia_monthly_cost_per_mbps > 0
				then (fy18.ia_monthly_cost_per_mbps - fy17.ia_monthly_cost_per_mbps)
		end as meeting_2019_goal_upgrade_cost_per_mbps_increase,
		case 
			when fy17.ia_monthly_cost_per_mbps > 0 and fy18.ia_monthly_cost_per_mbps > 0
				then (fy18.ia_monthly_cost_per_mbps - fy17.ia_monthly_cost_per_mbps)/fy17.ia_monthly_cost_per_mbps 
		end as meeting_2019_goal_upgrade_cost_per_mbps_increase_p,
		(fy18.fit_for_ia = true 
			and fy17.fit_for_ia = true
			and fy18.meeting_2018_goal_oversub = TRUE
			and fy17.meeting_2018_goal_oversub = FALSE
			and fy18.upgrade_indicator = TRUE)
		as newly_meeting_status,
		case 
			when fy18.fit_for_ia = true 
					and fy17.fit_for_ia = true
					and fy18.meeting_2018_goal_oversub = TRUE
					and fy17.meeting_2018_goal_oversub = FALSE
					and fy18.upgrade_indicator = TRUE
				then 'Newly Meeting'
			when fy18.fit_for_ia = true and fy18.meeting_2018_goal_oversub = TRUE
				then 'Already Meeting'
			when fy18.fit_for_ia = true and fy18.meeting_2018_goal_oversub = FALSE
				then 'Not Meeting'
			when  fy18.fit_for_ia = false 
				then 'Dirty'
		end as meeting_2019_status,
		fy18.upgrade_indicator 

		from ps.districts dd 

		inner join all_yrs fy18 
		on fy18.district_id = dd.district_id
		and fy18.funding_year = 2019

		left join all_yrs fy17 
		on fy17.district_id = dd.district_id
		and fy17.funding_year = 2018

		where dd.funding_year = 2019 

),

new_in_2018 as (select dd.district_id,
		dd.name,
		dd.state_code,
		dd.locale,
		dd.size,
		dd.latitude,
		dd.longitude,
		dd.num_students,
		(dd.num_students::numeric * dd.setda_concurrency_factor) as projected_bw_fy2019_no_rounding,

		fy17.ia_bw_mbps_total as new_ia_bw_mbps_total,
		fy17.meeting_2014_goal_no_oversub as new_meeting_2014_goal_no_oversub,
		fy17.meeting_2018_goal_oversub as new_meeting_2018_goal_oversub,
		fy17.ia_bandwidth_per_student_kbps as new_ia_bandwidth_per_student_kbps,
		fy17.ia_monthly_cost_total as new_ia_monthly_cost_total,
		fy17.ia_monthly_cost_per_mbps as new_ia_monthly_cost_per_mbps,
		fy17.consortium_affiliation as new_consortium_affiliation,
		fy17.primary_sp as new_primary_sp,

		fy16.ia_bw_mbps_total as old_ia_bw_mbps_total,
		fy16.meeting_2014_goal_no_oversub as old_meeting_2014_goal_no_oversub,
		fy16.meeting_2018_goal_oversub as old_meeting_2018_goal_oversub,
		fy16.ia_bandwidth_per_student_kbps as old_ia_bandwidth_per_student_kbps,
		fy16.ia_monthly_cost_total as old_ia_monthly_cost_total,
		fy16.ia_monthly_cost_per_mbps as old_ia_monthly_cost_per_mbps,
		fy16.consortium_affiliation as old_consortium_affiliation,
		fy16.primary_sp as old_primary_sp,

		case 
			when fy16.ia_bw_mbps_total > 0 and fy17.ia_bw_mbps_total > 0
				then (fy17.ia_bw_mbps_total - fy16.ia_bw_mbps_total)
		end as meeting_2019_goal_upgrade_bw_increase,
		case 
			when fy16.ia_bw_mbps_total > 0 and fy17.ia_bw_mbps_total > 0
				then (fy17.ia_bw_mbps_total - fy16.ia_bw_mbps_total)/fy16.ia_bw_mbps_total 
		end AS meeting_2019_goal_upgrade_bw_increase_p,
		case 
			when fy16.ia_monthly_cost_total > 0 and fy17.ia_monthly_cost_total > 0
				then (fy17.ia_monthly_cost_total - fy16.ia_monthly_cost_total) 
		end as meeting_2019_goal_upgrade_cost_increase,
		case 
			when fy16.ia_monthly_cost_total > 0 and fy17.ia_monthly_cost_total > 0
				then (fy17.ia_monthly_cost_total - fy16.ia_monthly_cost_total)/fy16.ia_monthly_cost_total 
		end as meeting_2019_goal_upgrade_cost_increase_p,
		(fy17.consortium_affiliation = fy16.consortium_affiliation) as same_consortia,
		(fy17.primary_sp = fy16.primary_sp) as same_primary_sp,
		case 
			when fy16.ia_monthly_cost_per_mbps > 0 and fy17.ia_monthly_cost_per_mbps > 0
				then (fy17.ia_monthly_cost_per_mbps - fy16.ia_monthly_cost_per_mbps)
		end as meeting_2019_goal_upgrade_cost_per_mbps_increase,
		case 
			when fy16.ia_monthly_cost_per_mbps > 0 and fy17.ia_monthly_cost_per_mbps > 0
				then (fy17.ia_monthly_cost_per_mbps - fy16.ia_monthly_cost_per_mbps)/fy16.ia_monthly_cost_per_mbps 
		end as meeting_2019_goal_upgrade_cost_per_mbps_increase_p,
		(fy17.fit_for_ia = true 
			and fy16.fit_for_ia = true
			and fy17.meeting_2018_goal_oversub = TRUE
			and fy16.meeting_2018_goal_oversub = FALSE
			and fy17.upgrade_indicator = true)
		as newly_meeting_status,
		case 
			when fy17.fit_for_ia = true 
					and fy16.fit_for_ia = true
					and fy17.meeting_2018_goal_oversub = TRUE
					and fy16.meeting_2018_goal_oversub = FALSE
					and fy17.upgrade_indicator = true
				then 'Newly Meeting'
			when fy17.fit_for_ia = true and fy17.meeting_2018_goal_oversub = TRUE
				then 'Already Meeting'
			when fy17.fit_for_ia = true and fy17.meeting_2018_goal_oversub = FALSE
				then 'Not Meeting'
			when  fy17.fit_for_ia = false 
				then 'Dirty'
		end as meeting_2019_status,
		fy17.upgrade_indicator


		from ps.districts dd 

		inner join all_yrs fy17 
		on fy17.district_id = dd.district_id
		and fy17.funding_year = 2018

		left join all_yrs fy16 
		on fy16.district_id = dd.district_id
		and fy16.funding_year = 2017

		where dd.funding_year = 2018 

	),

final_agg as (select 
	2019 as newly_meeting_year,
	fy18.district_id,
	fy18.name,
	fy18.state_code,
	fy18.locale,
	fy18.size,
	fy18.latitude::numeric,
	fy18.longitude::numeric,
	fy18.num_students,
	fy18.projected_bw_fy2019_no_rounding,
	case 
		when fy18.projected_bw_fy2019_no_rounding > 0 and fy18.old_ia_bw_mbps_total > 0
			then (fy18.projected_bw_fy2019_no_rounding - fy18.old_ia_bw_mbps_total) 
	end as additional_bw_needed_to_meet,
	case 
		when fy18.projected_bw_fy2019_no_rounding > 0 and fy18.old_ia_bw_mbps_total > 0
			then (fy18.projected_bw_fy2019_no_rounding - fy18.old_ia_bw_mbps_total)/fy18.old_ia_bw_mbps_total
	end as additional_bw_needed_to_meet_p,
	fy18.new_ia_bw_mbps_total,
	fy18.new_meeting_2014_goal_no_oversub,
	fy18.new_meeting_2018_goal_oversub,
	fy18.new_ia_bandwidth_per_student_kbps,
	fy18.new_ia_monthly_cost_total,
	fy18.new_ia_monthly_cost_per_mbps,
	fy18.new_consortium_affiliation,
	fy18.new_primary_sp,
	fy18.old_ia_bw_mbps_total,
	fy18.old_meeting_2014_goal_no_oversub,
	fy18.old_meeting_2018_goal_oversub,
	fy18.old_ia_bandwidth_per_student_kbps,
	fy18.old_ia_monthly_cost_total,
	fy18.old_ia_monthly_cost_per_mbps,
	fy18.old_consortium_affiliation,
	fy18.old_primary_sp,
	fy18.meeting_2019_goal_upgrade_bw_increase,
	fy18.meeting_2019_goal_upgrade_bw_increase_p,
	fy18.meeting_2019_goal_upgrade_cost_increase,
	fy18.meeting_2019_goal_upgrade_cost_increase_p,
	case 
		when fy18.meeting_2019_goal_upgrade_cost_increase_p <= .05 AND fy18.meeting_2019_goal_upgrade_cost_increase_p >= -.05
			then 'No Cost Change'
		when fy18.meeting_2019_goal_upgrade_cost_increase_p > .05
			then 'Cost Increase'
		when fy18.meeting_2019_goal_upgrade_cost_increase_p < -.05
			then 'Cost Decrease'
	end as meeting_2019_goal_upgrade_cost_increase_category,
	fy18.same_consortia,
	fy18.same_primary_sp,
	fy18.meeting_2019_goal_upgrade_cost_per_mbps_increase,
	fy18.meeting_2019_goal_upgrade_cost_per_mbps_increase_p,
	fy18.newly_meeting_status,
	fy18.meeting_2019_status,
	fy18.upgrade_indicator


	from new_in_2019 fy18

	union

	select 
	2018 as newly_meeting_year,
	fy17.district_id,
	fy17.name,
	fy17.state_code,
	fy17.locale,
	fy17.size,
	fy17.latitude::numeric,
	fy17.longitude::numeric,
	fy17.num_students,
	fy17.projected_bw_fy2019_no_rounding,
	case 
		when fy17.projected_bw_fy2019_no_rounding > 0 and fy17.old_ia_bw_mbps_total > 0
			then (fy17.projected_bw_fy2019_no_rounding - fy17.old_ia_bw_mbps_total) 
	end as additional_bw_needed_to_meet,
	case 
		when fy17.projected_bw_fy2019_no_rounding > 0 and fy17.old_ia_bw_mbps_total > 0
			then (fy17.projected_bw_fy2019_no_rounding - fy17.old_ia_bw_mbps_total)/fy17.old_ia_bw_mbps_total 
	end as additional_bw_needed_to_meet_p,
	fy17.new_ia_bw_mbps_total,
	fy17.new_meeting_2014_goal_no_oversub,
	fy17.new_meeting_2018_goal_oversub,
	fy17.new_ia_bandwidth_per_student_kbps,
	fy17.new_ia_monthly_cost_total,
	fy17.new_ia_monthly_cost_per_mbps,
	fy17.new_consortium_affiliation,
	fy17.new_primary_sp,
	fy17.old_ia_bw_mbps_total,
	fy17.old_meeting_2014_goal_no_oversub,
	fy17.old_meeting_2018_goal_oversub,
	fy17.old_ia_bandwidth_per_student_kbps,
	fy17.old_ia_monthly_cost_total,
	fy17.old_ia_monthly_cost_per_mbps,
	fy17.old_consortium_affiliation,
	fy17.old_primary_sp,
	fy17.meeting_2019_goal_upgrade_bw_increase,
	fy17.meeting_2019_goal_upgrade_bw_increase_p,
	fy17.meeting_2019_goal_upgrade_cost_increase,
	fy17.meeting_2019_goal_upgrade_cost_increase_p,
	case 
		when fy17.meeting_2019_goal_upgrade_cost_increase_p <= .05 AND fy17.meeting_2019_goal_upgrade_cost_increase_p >= -.05
			then 'No Cost Change'
		when fy17.meeting_2019_goal_upgrade_cost_increase_p > .05
			then 'Cost Increase'
		when fy17.meeting_2019_goal_upgrade_cost_increase_p < -.05
			then 'Cost Decrease'
	end as meeting_2019_goal_upgrade_cost_increase_category,
	fy17.same_consortia,
	fy17.same_primary_sp,
	fy17.meeting_2019_goal_upgrade_cost_per_mbps_increase,
	fy17.meeting_2019_goal_upgrade_cost_per_mbps_increase_p,
	fy17.newly_meeting_status,
	fy17.meeting_2019_status,
	fy17.upgrade_indicator


	from new_in_2018 as fy17
),

year_total as (select newly_meeting_year,
'Total'::varchar as district_group,
count(district_id) as districts,
1 as districts_p,
median(additional_bw_needed_to_meet) as median_additional_bw_needed_to_meet,
median(additional_bw_needed_to_meet_p) as median_additional_bw_needed_to_meet_p,
median(meeting_2019_goal_upgrade_bw_increase) as median_meeting_2019_goal_upgrade_bw_increase,
median(meeting_2019_goal_upgrade_bw_increase_p) as median_meeting_2019_goal_upgrade_bw_increase_p


from final_agg 

where newly_meeting_status = TRUE

group by newly_meeting_year
)


select s.newly_meeting_year,
s.size as district_group,
count(s.district_id) as districts,
count(s.district_id)::numeric/t.districts as districts_p,
median(s.additional_bw_needed_to_meet) as median_additional_bw_needed_to_meet,
median(s.additional_bw_needed_to_meet_p) as median_additional_bw_needed_to_meet_p,
median(s.meeting_2019_goal_upgrade_bw_increase) as median_meeting_2019_goal_upgrade_bw_increase,
median(s.meeting_2019_goal_upgrade_bw_increase_p) as median_meeting_2019_goal_upgrade_bw_increase_p,
1 as order_group 


from final_agg s

left join year_total t 
on t.newly_meeting_year = s.newly_meeting_year


where newly_meeting_status = TRUE

group by s.newly_meeting_year,
s.size,
t.districts


union 

select s.newly_meeting_year,
case 
	when s.size in ('Tiny','Small')
		then 'Tiny & Small'
	when s.size in ('Medium','Large','Mega')
		then 'Medium, Large & Mega' 
end as district_group,
count(s.district_id) as districts,
count(s.district_id)::numeric/t.districts as districts_p,
median(s.additional_bw_needed_to_meet) as median_additional_bw_needed_to_meet,
median(s.additional_bw_needed_to_meet_p) as median_additional_bw_needed_to_meet_p,
median(s.meeting_2019_goal_upgrade_bw_increase) as median_meeting_2019_goal_upgrade_bw_increase,
median(s.meeting_2019_goal_upgrade_bw_increase_p) as median_meeting_2019_goal_upgrade_bw_increase_p,
2 as order_group


from final_agg s

left join year_total t 
on t.newly_meeting_year = s.newly_meeting_year


where newly_meeting_status = TRUE

group by 1,2,t.districts

union 

select *,
3 as order_group
from year_total

order by order_group,median_additional_bw_needed_to_meet, district_group,newly_meeting_year


