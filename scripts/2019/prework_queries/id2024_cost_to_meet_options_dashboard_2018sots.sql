with group_dd as (select 
	group_dd.district_id, 
	group_dd.funding_year,
	fit.fit_for_ia,
	fit.fit_for_ia_cost,
	CASE 
		WHEN fit.fit_for_ia = TRUE
			THEN bc.meeting_2018_goal_oversub
	END as meeting_2018_goal_oversub,
	CASE 
		WHEN fit.fit_for_ia_cost = TRUE 
			THEN bc.ia_monthly_cost_total
	END as ia_monthly_cost_total,
	(group_dd.num_students::numeric * group_dd.setda_concurrency_factor) as projected_bw_fy2019_no_rounding,
	bc.ia_monthly_cost_per_mbps

	
 	FROM ps.districts group_dd 

 	JOIN ps.districts_fit_for_analysis fit
 	ON fit.district_id = group_dd.district_id
 	AND fit.funding_year = group_dd.funding_year

 	JOIN ps.districts_bw_cost bc
	ON group_dd.district_id = bc.district_id
	AND group_dd.funding_year = bc.funding_year

 	WHERE group_dd.in_universe = true
 	AND group_dd.district_type = 'Traditional'
 	and group_dd.funding_year = 2019

 	),

unnested_consortia as (select 
	funding_year,
	consortia_id::int, 
	district_id

	from ps.districts , unnest (string_to_array(consortium_affiliation_ids, ' | ')) s(consortia_id)

	where funding_year = 2019
	and consortium_affiliation_ids is not null 
),

consortia_agg1 as (select 
		group_dd.funding_year,
		group_dd.district_id,
		group_dd.fit_for_ia,
		group_dd.fit_for_ia_cost,
		group_dd.meeting_2018_goal_oversub,
		unnested_consortia.consortia_id,
		case 
			when unnested_consortia.consortia_id is not null
				then count(case 
					when meeting_2018_goal_oversub = true 
						then group_dd.district_id end) 
				over (partition by unnested_consortia.consortia_id) 
		end as consortia_districts_meeting_2019,
		case 
			when count(case 
								when meeting_2018_goal_oversub is not null 
									then group_dd.district_id end) 
							over (partition by unnested_consortia.consortia_id) > 0
				then 
					count(case 
								when meeting_2018_goal_oversub = true 
									then group_dd.district_id end) 
							over (partition by unnested_consortia.consortia_id)::numeric/
					count(case 
								when meeting_2018_goal_oversub is not null 
									then group_dd.district_id end) 
							over (partition by unnested_consortia.consortia_id)::numeric
		end consortia_districts_meeting_2019_p,
		count( group_dd.district_id) over (partition by unnested_consortia.consortia_id) as consortia_districts

		from group_dd 

		left join unnested_consortia
		on unnested_consortia.district_id = group_dd.district_id 
		and unnested_consortia.consortia_id is not null
),

/* removing any consortia that only serve one district from data */
consortia_agg2 as (select 
		funding_year,
		district_id,
		fit_for_ia,
		fit_for_ia_cost,
		meeting_2018_goal_oversub,
		case 
			when consortia_districts = 1
				then null 
			else consortia_id
		end as consortia_id,
		case 
			when consortia_districts = 1 
				then null 
			else consortia_districts_meeting_2019
		end as consortia_districts_meeting_2019,
		case 
			when consortia_districts = 1 
				then null 
			else consortia_districts_meeting_2019_p
		end as consortia_districts_meeting_2019_p,
		case 
			when consortia_districts = 1 
				then null 
			else consortia_districts
		end as consortia_districts

		from consortia_agg1

),

no_cost_peer as (select distinct on (group_dd.district_id)
		group_dd.funding_year,
		group_dd.district_id,
		p.peer_id,
		pd.ia_monthly_cost_total - group_dd.ia_monthly_cost_total as mrc_increase

		from group_dd 

		inner join ps.districts_peers_ranks  p
		ON group_dd.district_id = p.district_id
		AND group_dd.funding_year = p.funding_year

		inner join group_dd pd 
		on pd.district_id = p.peer_id
		and pd.funding_year = p.funding_year 

		where group_dd.fit_for_ia_cost = true 
		and group_dd.meeting_2018_goal_oversub = false 
		and group_dd.ia_monthly_cost_per_mbps is not null
		and p.peer_ia_bw_mbps_total >= group_dd.projected_bw_fy2019_no_rounding

		-- get the worst deal 
		order by group_dd.district_id, pd.ia_monthly_cost_total desc
),

district_agg as (select group_dd.funding_year,
		group_dd.district_id,
		group_dd.fit_for_ia,
		group_dd.fit_for_ia_cost,
		group_dd.meeting_2018_goal_oversub,
		group_dd.ia_monthly_cost_total,
		case 
			when (peerdeal.district_id is not null) 
				then 'Peer Deal'
			when count(case 
					when consortia_districts_meeting_2019_p >= .1 
						then ca.consortia_id end) > 0 
				then 'Model Consortia'
			else 'Hard to Meet Group'
		end as district_group
		
		from group_dd 

		left join no_cost_peer peerdeal 
		on peerdeal.district_id = group_dd.district_id

		left join consortia_agg2 ca 
		on ca.district_id = group_dd.district_id

		where group_dd.meeting_2018_goal_oversub = false

		group by group_dd.funding_year,
		group_dd.district_id,
		group_dd.fit_for_ia,
		group_dd.fit_for_ia_cost,
		group_dd.meeting_2018_goal_oversub,
		group_dd.ia_monthly_cost_total,
		peerdeal.district_id,
		ca.district_id

),

already_meeting_cost as (
	select sum(bc.ia_monthly_cost_total)/sum(bc.ia_bw_mbps_total) as weighted_avg_cost_per_mbps,
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

),

district_costs as (
	select dd.district_id,
	dd.funding_year,
	dd.num_students,
	district_agg.district_group,
	bc.ia_bw_mbps_total,
	bc.projected_bw_fy2019_cck12,
	bc.projected_bw_fy2019_cck12 - bc.ia_bw_mbps_total as extra_bw_needed_to_meet,

	---current cost 
	bc.ia_monthly_cost_per_mbps,
	bc.ia_monthly_cost_total as current_mrc,
	bc.ia_annual_cost_total as current_annual_cost,
	bc.ia_funding_requested_erate as current_annual_erate_cost,
	(bc.ia_annual_cost_total - bc.ia_funding_requested_erate) as current_annual_oop_cost,

	---knapsack
	ps.knapsack_budget(bc.projected_bw_fy2019_cck12) as knapsack_mrc,
	ps.knapsack_budget(bc.projected_bw_fy2019_cck12)*12 as knapsack_annual_cost,
	(ps.knapsack_budget(bc.projected_bw_fy2019_cck12)*12)*(bc.ia_funding_requested_erate/bc.ia_annual_cost_total) as knapsack_annual_erate_cost,
	(ps.knapsack_budget(bc.projected_bw_fy2019_cck12)*12)*(((bc.ia_annual_cost_total - bc.ia_funding_requested_erate))/bc.ia_annual_cost_total) as knapsack_annual_oop_cost,

	---already meeting weighted average 
	bc.projected_bw_fy2019_cck12*already_meeting_cost.weighted_avg_cost_per_mbps as am_wa_mrc,
	bc.projected_bw_fy2019_cck12*already_meeting_cost.weighted_avg_cost_per_mbps*12 as am_wa_annual_cost,
	(bc.projected_bw_fy2019_cck12*already_meeting_cost.weighted_avg_cost_per_mbps*12)* (bc.ia_funding_requested_erate/bc.ia_annual_cost_total) as am_wa_annual_erate_cost,
	(bc.projected_bw_fy2019_cck12*already_meeting_cost.weighted_avg_cost_per_mbps*12)*(((bc.ia_annual_cost_total - bc.ia_funding_requested_erate))/bc.ia_annual_cost_total) as am_wa_annual_oop_cost,

	--already meeting median
	bc.projected_bw_fy2019_cck12*already_meeting_cost.median_cost_per_mbps as am_median_mrc,
	bc.projected_bw_fy2019_cck12*already_meeting_cost.median_cost_per_mbps*12 as am_median_annual_cost,
	(bc.projected_bw_fy2019_cck12*already_meeting_cost.median_cost_per_mbps*12)* (bc.ia_funding_requested_erate/bc.ia_annual_cost_total) as am_median_annual_erate_cost,
	(bc.projected_bw_fy2019_cck12*already_meeting_cost.median_cost_per_mbps*12)*(((bc.ia_annual_cost_total - bc.ia_funding_requested_erate))/bc.ia_annual_cost_total) as am_median_annual_oop_cost


	FROM ps.districts dd 

	JOIN ps.districts_fit_for_analysis fit
	ON fit.district_id = dd.district_id
	AND fit.funding_year = dd.funding_year

	JOIN ps.districts_bw_cost bc
	ON dd.district_id = bc.district_id
	AND dd.funding_year = bc.funding_year

	JOIN already_meeting_cost 
	ON true

	JOIN district_agg 
	ON district_agg.district_id = dd.district_id
	and district_agg.funding_year = dd.funding_year

	WHERE dd.funding_year = 2019
	and dd.in_universe = true
	and dd.district_type = 'Traditional'
	and fit.fit_for_ia = true
	and fit.fit_for_ia_cost = true 
	and bc.meeting_2018_goal_oversub = false 
	and district_agg.district_group = 'Hard to Meet Group'

	and bc.ia_annual_cost_total > 0
	/*excluding free or restricted cost districts -  they are unusual/outlier scenarios */
)

select 
district_id,
num_students,
projected_bw_fy2019_cck12,
extra_bw_needed_to_meet,

(knapsack_annual_oop_cost - current_annual_oop_cost)/num_students as extra_knapsack_annual_oop_cost_student,
(knapsack_annual_oop_cost - current_annual_oop_cost) as extra_knapsack_annual_oop_cost,
(knapsack_annual_oop_cost - current_annual_oop_cost) < 0 as  knapsack_no_cost_group,

(am_wa_annual_oop_cost - current_annual_oop_cost)/num_students as extra_am_wa_annual_oop_cost_student,
(am_wa_annual_oop_cost - current_annual_oop_cost) as extra_am_wa_annual_oop_cost,
(am_wa_annual_oop_cost - current_annual_oop_cost) < 0 as am_wa_no_cost_group,

(am_median_annual_oop_cost - current_annual_oop_cost)/num_students as extra_am_median_annual_oop_cost_student,
(am_median_annual_oop_cost - current_annual_oop_cost) as extra_am_median_annual_oop_cost,
(am_median_annual_oop_cost - current_annual_oop_cost) < 0 as am_median_no_cost_group

from district_costs


