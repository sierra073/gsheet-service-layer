with all_yrs as (select 
	dd.district_id, 
	dd.funding_year,
	fit.fit_for_ia,
	fit.fit_for_ia_cost,
	projected_bw_fy2019_cck12,
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
	END as ia_monthly_cost_per_mbps,
	CASE 
		WHEN fit.fit_for_ia = TRUE and fit_older.fit_for_ia = TRUE
			THEN du.upgrade_indicator
	END as upgrade_indicator,
	CASE
		WHEN fit.fit_for_ia_cost = TRUE
			THEN bc.ia_annual_cost_total
	END as ia_annual_cost_total,
	CASE
		WHEN fit.fit_for_ia_cost = TRUE
			THEN bc.ia_funding_requested_erate
	END as ia_funding_requested_erate,
	CASE 
		WHEN fit.fit_for_ia_cost = TRUE 
			THEN bc.ia_annual_cost_total - bc.ia_funding_requested_erate
	END as ia_annual_cost_oop

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

 	),

dd as (select  
		dd.funding_year,
		dd.district_id,
		dd.name,
		dd.state_code,
		dd.locale,
		dd.size,
		dd.latitude,
		dd.longitude,
		dd.num_students,
		fy18.fit_for_ia,
		fy18.fit_for_ia_cost,
		fy18.projected_bw_fy2019_cck12,
		(dd.num_students::numeric * dd.setda_concurrency_factor) as projected_bw_fy2019_no_rounding,
		case 
			when fy18.ia_bw_mbps_total is not null
				then (dd.num_students::numeric * dd.setda_concurrency_factor) - fy18.ia_bw_mbps_total 
		end as bw_needed_to_meet,
		fy18.ia_bw_mbps_total,
		fy18.meeting_2014_goal_no_oversub,
		fy18.meeting_2018_goal_oversub,
		fy18.ia_bandwidth_per_student_kbps,
		fy18.ia_monthly_cost_total,
		fy18.ia_monthly_cost_per_mbps,
		case 
			when fy17.ia_monthly_cost_total is not null and fy18.ia_monthly_cost_total is not null
				then (fy18.ia_monthly_cost_total - fy17.ia_monthly_cost_total)
		end as mrc_increase,
		case 
			when fy17.ia_monthly_cost_total is not null and fy18.ia_monthly_cost_total is not null
				then (fy18.ia_monthly_cost_total - fy17.ia_monthly_cost_total)/dd.num_students
		end as mrc_increase_per_student,
		(fy18.fit_for_ia_cost = TRUE 
			and fy17.fit_for_ia_cost = TRUE
			and fy18.meeting_2018_goal_oversub = TRUE
			and fy17.meeting_2018_goal_oversub = FALSE
			and fy18.upgrade_indicator = TRUE)
		as newly_meeting_status,
		fy18.ia_annual_cost_total,
		fy18.ia_funding_requested_erate,	
		fy18.ia_annual_cost_oop,
		case 
			when fy17.ia_annual_cost_oop is not null and fy18.ia_annual_cost_oop is not null
				then (fy18.ia_annual_cost_oop - fy17.ia_annual_cost_oop)/dd.num_students
		end as annual_oop_increase_per_student,
		case 
			when fy17.ia_annual_cost_total is not null and fy18.ia_annual_cost_total is not null
				then (fy18.ia_annual_cost_total - fy17.ia_annual_cost_total)/dd.num_students
		end as annual_cost_increase_per_student

		from ps.districts dd 

		inner join all_yrs fy18 
		on fy18.district_id = dd.district_id
		and fy18.funding_year = 2019

		left join all_yrs fy17 
		on fy17.district_id = dd.district_id
		and fy17.funding_year = 2018

		where dd.funding_year = 2019 

),


newly_meeting_cost_pre as (
	select 
	median(mrc_increase_per_student) as median_mrc_increase_per_student,
	percentile_cont(.8) within group (order by mrc_increase_per_student) as p80_mrc_increase_per_student,
	median(annual_oop_increase_per_student) as median_annual_oop_increase_per_student,
	percentile_cont(.8) within group (order by annual_oop_increase_per_student) as p80_annual_oop_increase_per_student,
	median(annual_cost_increase_per_student) as median_annual_cost_increase_per_student,
	percentile_cont(.8) within group (order by annual_cost_increase_per_student) as p80_annual_cost_increase_per_student,
	count(district_id) as districts

	from dd 

	where newly_meeting_status = TRUE

),


-- planning to switch out the percent upgraded to meet group for the other 2 scenarios

cost_decrease_percents as (
	select 1 as year,
	.781 as percent_cost_decrease,
	(1::numeric/3) as percent_upgraded_to_meet
	--.69 as percent_upgraded_to_meet
	--.04 as percent_upgraded_to_meet

	from newly_meeting_cost_pre

	union 

	select 2 as year,
	.6623 as percent_cost_decrease,
	(1::numeric/3) as percent_upgraded_to_meet
	--.24 as percent_upgraded_to_meet
	--.19 as percent_upgraded_to_meet

	from newly_meeting_cost_pre

	union 

	select 3 as year,
	.5979 as percent_cost_decrease,
	(1::numeric/3) as percent_upgraded_to_meet
	--.07 as percent_upgraded_to_meet
	--.77 as percent_upgraded_to_meet

	from newly_meeting_cost_pre
),

newly_meeting_cost as (
	/*leaving these year 0 numbers in case i want to reference them but they will not be used in any calculations*/
		select 0 as year,
		median_mrc_increase_per_student,
		p80_mrc_increase_per_student,
		median_annual_oop_increase_per_student,
		p80_annual_oop_increase_per_student,
		median_annual_cost_increase_per_student,
		p80_annual_cost_increase_per_student,
		null as percent_upgraded_to_meet

		from newly_meeting_cost_pre

	union

		select year,
		median_mrc_increase_per_student*percent_cost_decrease as median_mrc_increase_per_student,
		p80_mrc_increase_per_student*percent_cost_decrease as p80_mrc_increase_per_student,
		median_annual_oop_increase_per_student*percent_cost_decrease as median_annual_oop_increase_per_student,
		p80_annual_oop_increase_per_student*percent_cost_decrease as p80_annual_oop_increase_per_student,
		median_annual_cost_increase_per_student*percent_cost_decrease as median_annual_cost_increase_per_student,
		p80_annual_cost_increase_per_student*percent_cost_decrease as p80_annual_cost_increase_per_student,
		percent_upgraded_to_meet

		from newly_meeting_cost_pre

		join cost_decrease_percents
		on TRUE
),


pre_unnested_consortia as (select 
	funding_year,
	consortia_id::int, 
	district_id

	from ps.districts , unnest (string_to_array(consortium_affiliation_ids, ' | ')) s(consortia_id)


	where funding_year = 2019
	and consortium_affiliation_ids is not null 
),

unnested_consortia as (select pre_unnested_consortia.funding_year,
	pre_unnested_consortia.district_id,
	pre_unnested_consortia.consortia_id,
	(pre_unnested_consortia.consortia_id in (1005870,1006162,1004592,1008357,1014118,1015511,1020107,1016880,1023114,1020220,1037707,1035776,
											1055594,1051850,1051855,1041650,1031023,1009239,1047587,1047087,1032821,1021110,1051767,1055045,1049045)
	) as state_network,
	consortia_name

	from pre_unnested_consortia

	left join ps.consortia c 
	on c.consortia_id = pre_unnested_consortia.consortia_id
	and c.funding_year = pre_unnested_consortia.funding_year
),

consortia_agg1 as (select 
		dd.funding_year,
		dd.district_id,
		dd.name,
		dd.state_code,
		dd.locale,
		dd.size,
		dd.latitude,
		dd.longitude,
		dd.num_students,
		dd.fit_for_ia,
		dd.fit_for_ia_cost,
		dd.projected_bw_fy2019_cck12,
		dd.projected_bw_fy2019_no_rounding,
		dd.bw_needed_to_meet,
		dd.ia_bw_mbps_total,
		dd.meeting_2014_goal_no_oversub,
		dd.meeting_2018_goal_oversub,
		dd.ia_bandwidth_per_student_kbps,
		dd.ia_monthly_cost_total,
		dd.ia_monthly_cost_per_mbps,
		unnested_consortia.consortia_id,
		unnested_consortia.state_network,
		unnested_consortia.consortia_name,
		case 
			when unnested_consortia.consortia_id is not null
				then count(case 
					when meeting_2018_goal_oversub = true 
						then dd.district_id end) 
				over (partition by unnested_consortia.consortia_id) 
		end as consortia_districts_meeting_2019,
		case 
			when count(case 
								when meeting_2018_goal_oversub is not null 
									then dd.district_id end) 
							over (partition by unnested_consortia.consortia_id) > 0
				then 
					count(case 
								when meeting_2018_goal_oversub = true 
									then dd.district_id end) 
							over (partition by unnested_consortia.consortia_id)::numeric/
					count(case 
								when meeting_2018_goal_oversub is not null 
									then dd.district_id end) 
							over (partition by unnested_consortia.consortia_id)::numeric
		end consortia_districts_meeting_2019_p,
		count( dd.district_id) over (partition by unnested_consortia.consortia_id) as consortia_districts

		from dd 

		left join unnested_consortia
		on unnested_consortia.district_id = dd.district_id 
		and unnested_consortia.consortia_id is not null
),

/* removing any consortia that only serve one district from data */
consortia_agg2 as (select 
		funding_year,
		district_id,
		name,
		state_code,
		locale,
		size,
		latitude,
		longitude,
		num_students,
		fit_for_ia,
		fit_for_ia_cost,
		projected_bw_fy2019_cck12,
		projected_bw_fy2019_no_rounding,
		bw_needed_to_meet,
		ia_bw_mbps_total,
		meeting_2014_goal_no_oversub,
		meeting_2018_goal_oversub,
		ia_bandwidth_per_student_kbps,
		ia_monthly_cost_total,
		ia_monthly_cost_per_mbps,
		case 
			when consortia_districts = 1
				then null 
			else consortia_id
		end as consortia_id,
		case 
			when consortia_districts = 1
				then null
			else state_network
		end as state_network,
		case 
			when consortia_districts = 1 
				then null 
			else consortia_name
		end as consortia_name,
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

no_cost_peer as (select distinct on (dd.district_id)
		dd.funding_year,
		dd.district_id,
		p.peer_id,
		pd.ia_monthly_cost_total - dd.ia_monthly_cost_total as mrc_increase,
		pd.ia_annual_cost_total - dd.ia_annual_cost_total as annual_cost_increase,
		pd.ia_annual_cost_oop - dd.ia_annual_cost_oop as annual_oop_increase

		from dd 

		inner join ps.districts_peers_ranks  p
		ON dd.district_id = p.district_id
		AND dd.funding_year = p.funding_year

		inner join all_yrs pd 
		on pd.district_id = p.peer_id
		and pd.funding_year = p.funding_year 

		where dd.fit_for_ia_cost = true 
		and dd.meeting_2018_goal_oversub = false 
		and dd.ia_monthly_cost_per_mbps is not null
		and p.peer_ia_bw_mbps_total >= dd.projected_bw_fy2019_no_rounding


		-- get the worst deal 
		order by dd.district_id, pd.ia_monthly_cost_total desc
),

cost_cck12_peer as ( select distinct on (dd.district_id)
		dd.funding_year,
		dd.district_id,
    	p.peer_id,
    	pd.ia_monthly_cost_total - dd.ia_monthly_cost_total as mrc_increase,
		pd.ia_annual_cost_total - dd.ia_annual_cost_total as annual_cost_increase,
		pd.ia_annual_cost_oop - dd.ia_annual_cost_oop as annual_oop_increase

  		from dd 

		join (
		    select 
		      district_id,
		      funding_year,
		      unnest(bandwidth_suggested_districts) as peer_id
		    from ps.districts_peers 
		  ) p

	  	on p.district_id = dd.district_id
	  	and p.funding_year = dd.funding_year

	  	join all_yrs pd 
	  	on pd.district_id = p.peer_id
	  	and pd.funding_year = p.funding_year
	  
	  	where dd.fit_for_ia_cost = true 
		and dd.meeting_2018_goal_oversub = false 
		and dd.ia_monthly_cost_per_mbps is not null

	  	and dd.fit_for_ia_cost = true 
		and dd.meeting_2018_goal_oversub = false 
		and dd.ia_monthly_cost_per_mbps is not null
		and pd.ia_bw_mbps_total >= dd.projected_bw_fy2019_no_rounding

	  	and pd.fit_for_ia_cost = true
	  	--don't want any districts that already have a no cost peer deal
	  	and dd.district_id not in (select district_id from no_cost_peer)

	  	-- get the worst deal
	  	order by dd.district_id, pd.ia_monthly_cost_total desc
),

district_agg as (select ca.funding_year,
		ca.district_id,
		ca.name,
		ca.state_code,
		ca.locale,
		ca.size,
		ca.latitude,
		ca.longitude,
		ca.num_students,
		ca.fit_for_ia,
		ca.fit_for_ia_cost,
		ca.projected_bw_fy2019_cck12,
		ca.projected_bw_fy2019_no_rounding,
		ca.bw_needed_to_meet,
		ca.ia_bw_mbps_total,
		ca.meeting_2014_goal_no_oversub,
		ca.meeting_2018_goal_oversub,
		ca.ia_bandwidth_per_student_kbps,
		ca.ia_monthly_cost_total,
		count(ca.consortia_id) as consortia_count,
		count(ca.consortia_id)>0 as consortia,
		count(case 
			when consortia_districts_meeting_2019_p >= .1 
				then ca.consortia_id 
			end) 
		as consortia_w_some_already_meeting_count,
		count(case 
			when consortia_districts_meeting_2019_p >= .1 
				then ca.consortia_id end) > 0 
		as consortia_already_meeting,
		count(case 
			when consortia_districts_meeting_2019_p >= .1 and state_network = true 
				then ca.consortia_id end) > 0 
		as state_network_already_meeting,
		(peerdeal.district_id is not null) as no_cost_peer_deal,
		(cost_cck12_peer.district_id is not null) as cost_cck12_peer_deal,
		--mrc
		case 
			when ca.meeting_2018_goal_oversub = true or ca.fit_for_ia = false
				then null
			when peerdeal.district_id is not null 
				then peerdeal.mrc_increase 
			/*removing this since we don't use it in the model*/
			--when cost_cck12_peer.district_id is not null 
			--	then cost_cck12_peer.mrc_increase
			else sum((num_students*percent_upgraded_to_meet)*p80_mrc_increase_per_student)
		end as mrc_increase,
		-- oop 
		case 
			when ca.meeting_2018_goal_oversub = true or ca.fit_for_ia = false 
				then null
			when peerdeal.district_id is not null 
				then peerdeal.annual_oop_increase
			/*removing this since we don't use it in the model*/
			--when cost_cck12_peer.district_id is not null
			--	then cost_cck12_peer.annual_oop_increase
			else sum((num_students*percent_upgraded_to_meet)*p80_annual_oop_increase_per_student)
		end as annual_oop_increase,
		--
		case 
			when ca.meeting_2018_goal_oversub = true or ca.fit_for_ia = false 
				then null
			when peerdeal.district_id is not null 
				then peerdeal.annual_cost_increase
			/*removing this since we don't use it in the model*/
			---when cost_cck12_peer.district_id is not null 
			---	then cost_cck12_peer.annual_cost_increase
			else sum((num_students*percent_upgraded_to_meet)*p80_annual_cost_increase_per_student)
		end as annual_cost_increase

		from consortia_agg2 ca 

		left join no_cost_peer peerdeal 
		on peerdeal.district_id = ca.district_id

		left join cost_cck12_peer 
		on  cost_cck12_peer.district_id = ca.district_id

		join newly_meeting_cost 
		on year > 0 

		--adding methodology from second iteration of this analysis to get actual #s to match
		where ca.fit_for_ia = true 
		and ca.fit_for_ia_cost = true
		and ca.ia_monthly_cost_total > 0
		and ca.meeting_2018_goal_oversub = false


		group by ca.funding_year,
		ca.district_id,
		ca.name,
		ca.state_code,
		ca.locale,
		ca.size,
		ca.latitude,
		ca.longitude,
		ca.num_students,
		ca.fit_for_ia,
		ca.fit_for_ia_cost,
		ca.projected_bw_fy2019_cck12,
		ca.projected_bw_fy2019_no_rounding,
		ca.bw_needed_to_meet,
		ca.ia_bw_mbps_total,
		ca.meeting_2014_goal_no_oversub,
		ca.meeting_2018_goal_oversub,
		ca.ia_bandwidth_per_student_kbps,
		ca.ia_monthly_cost_total,
		peerdeal.district_id,
		peerdeal.mrc_increase,
		peerdeal.annual_oop_increase,
		peerdeal.annual_cost_increase,
		cost_cck12_peer.district_id,
		cost_cck12_peer.mrc_increase,
		cost_cck12_peer.annual_oop_increase,
		cost_cck12_peer.annual_cost_increase

),

extrap_before_1 as (select 
	count(district_id) as districts_pop,
	sum(num_students) as students_pop,
	count(case 
		when meeting_2018_goal_oversub = FALSE
			then district_id
		end)::numeric/count(case 
		when meeting_2018_goal_oversub is not null
			then district_id
		end) as not_meeting_2019_districts_p,
	sum(case 
		when meeting_2018_goal_oversub = FALSE
			then num_students
		end)::numeric/sum(case 
		when meeting_2018_goal_oversub is not null
			then num_students
		end) as not_meeting_2019_students_p,

	count(case 
		when meeting_2018_goal_oversub = TRUE
			then district_id
		end)::numeric/count(case 
		when meeting_2018_goal_oversub is not null
			then district_id
		end) as meeting_2019_districts_p,
	sum(case 
		when meeting_2018_goal_oversub = TRUE
			then num_students
		end)::numeric/sum(case 
		when meeting_2018_goal_oversub is not null
			then num_students
		end) as meeting_2019_students_p

	from district_agg
),

extrap_before_2 as (select *,
	round(districts_pop*not_meeting_2019_districts_p) as districts_not_meeting_extrap,
	round(students_pop*not_meeting_2019_students_p) as students_not_meeting_extrap,
	round(districts_pop*meeting_2019_districts_p) as districts_meeting_extrap,
	round(students_pop*meeting_2019_students_p) as students_meeting_extrap

	from extrap_before_1
),

extrap_after_1 as (	
		select 'peer_deal_no_cost' as upgrade_group,
		count(case
			when no_cost_peer_deal = TRUE
				then district_id end)::numeric/count(district_id) as districts_upgrade_p,
		sum(case
			when no_cost_peer_deal = TRUE
				then num_students end)::numeric/sum(num_students) as students_upgrade_p,
		sum(case 
			when no_cost_peer_deal = TRUE
				then mrc_increase end) as upgrade_mrc_actual,
		sum(case 
			when no_cost_peer_deal = TRUE
				then annual_oop_increase end) as upgrade_annual_oop_actual,
		sum(case 
			when no_cost_peer_deal = TRUE
				then annual_cost_increase end) as upgrade_annual_cost_actual,
		count(case
			when no_cost_peer_deal = TRUE
				then district_id end)::numeric as districts_upgrade_actual,
		sum(case
			when no_cost_peer_deal = TRUE
				then num_students end)::numeric as students_upgrade_actual

		from district_agg 

		where meeting_2018_goal_oversub = FALSE 


	union 

		select 'consortia_no_peer_deal' as upgrade_group,
		count(case
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = TRUE
				then district_id end)::numeric/count(district_id) as districts_upgrade_p,
		sum(case
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = TRUE
				then num_students end)::numeric/sum(num_students) as students_upgrade_p,
		sum(case 
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = TRUE
				then mrc_increase end) as upgrade_mrc_actual,
		sum(case 
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = TRUE
				then annual_oop_increase end) as upgrade_annual_oop_actual,
		sum(case 
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = TRUE
				then annual_cost_increase end) as upgrade_annual_cost_actual,
		count(case
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = TRUE
				then district_id end)::numeric as districts_upgrade_actual,
		sum(case
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = TRUE
				then num_students end)::numeric as students_upgrade_actual

		from district_agg 

		where meeting_2018_goal_oversub = FALSE 

	union

		select 'no_consortia_or_peer_deal' as upgrade_group,
		count(case
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = FALSE
				then district_id end)::numeric/count(district_id) as districts_upgrade_p,
		sum(case
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = FALSE
				then num_students end)::numeric/sum(num_students) as students_upgrade_p,
		sum(case 
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = FALSE
				then mrc_increase end) as upgrade_mrc_actual,
		sum(case 
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = FALSE
				then annual_oop_increase end) as upgrade_annual_oop_actual,
		sum(case 
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = FALSE
				then annual_cost_increase end) as upgrade_annual_cost_actual,
		count(case
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = FALSE
				then district_id end)::numeric as districts_upgrade_actual,
		sum(case
			when no_cost_peer_deal = FALSE AND consortia_already_meeting = FALSE
				then num_students end)::numeric as students_upgrade_actual

		from district_agg 

		where meeting_2018_goal_oversub = FALSE 

),

final_before_cost as (
	select 
	districts_pop,
	students_pop,
	not_meeting_2019_districts_p as not_meeting_2019_districts_before_p,
	not_meeting_2019_students_p as not_meeting_2019_students_before_p,
	meeting_2019_districts_p as meeting_2019_districts_before_p,
	meeting_2019_students_p as meeting_2019_students_before_p,
	districts_not_meeting_extrap as districts_not_meeting_before_extrap, 
	students_not_meeting_extrap as students_not_meeting_before_extrap,
	districts_meeting_extrap as districts_meeting_before_extrap,
	students_meeting_extrap as students_meeting_before_extrap,
	
	upgrade_group,
	districts_upgrade_p,
	students_upgrade_p,
	districts_upgrade_actual,
	students_upgrade_actual,
	upgrade_mrc_actual,
	upgrade_annual_oop_actual,
	upgrade_annual_cost_actual,

	round(districts_not_meeting_extrap*districts_upgrade_p) as districts_upgrade_extrap,
	round(students_not_meeting_extrap*students_upgrade_p) as students_upgrade_extrap,
	round(students_not_meeting_extrap*students_upgrade_p) - students_upgrade_actual as students_upgrade_only_extrap,

	(round(districts_not_meeting_extrap*districts_upgrade_p)+districts_meeting_extrap)/districts_pop as districts_meeting_after_upgrade_p,
	(round(students_not_meeting_extrap*students_upgrade_p)+students_meeting_extrap)/students_pop as students_meeting_after_upgrade_p,
	round(districts_not_meeting_extrap*districts_upgrade_p)+districts_meeting_extrap as districts_meeting_after_upgrade_extrap,
	round(students_not_meeting_extrap*students_upgrade_p)+students_meeting_extrap as students_meeting_after_upgrade_extrap,

	(districts_not_meeting_extrap - round(districts_not_meeting_extrap*districts_upgrade_p))/districts_pop as districts_not_meeting_after_upgrade_p,
	(students_not_meeting_extrap - round(students_not_meeting_extrap*students_upgrade_p))/students_pop as students_not_meeting_after_upgrade_p,
	(districts_not_meeting_extrap - round(districts_not_meeting_extrap*districts_upgrade_p)) as districts_not_meeting_after_upgrade_extrap,
	(students_not_meeting_extrap - round(students_not_meeting_extrap*students_upgrade_p)) as students_not_meeting_after_upgrade_extrap


	from extrap_before_2

	join extrap_after_1 
	on TRUE
)


select 
f.upgrade_group,
f.districts_upgrade_p,
f.students_upgrade_p,
f.districts_upgrade_actual,
f.students_upgrade_actual,
f.districts_upgrade_extrap,
f.students_upgrade_extrap,
f.students_upgrade_only_extrap,
--MRC --- extrapolating costs differently for districts with peer deals
f.upgrade_mrc_actual,
case 
	when upgrade_group = 'peer_deal_no_cost'
		then (f.upgrade_mrc_actual/f.students_upgrade_actual)*f.students_upgrade_only_extrap
	else sum((students_upgrade_only_extrap*percent_upgraded_to_meet)*p80_mrc_increase_per_student) 
end as upgrade_mrc_extrap_only,
case 
	when upgrade_group = 'peer_deal_no_cost'
		then (f.upgrade_mrc_actual/f.students_upgrade_actual)*f.students_upgrade_extrap
	else sum((students_upgrade_only_extrap*percent_upgraded_to_meet)*p80_mrc_increase_per_student) + f.upgrade_mrc_actual 
end as upgrade_mrc_total,
--Annual OOP --- extrapolating costs differently for districts with peer deals
f.upgrade_annual_oop_actual,
case 
	when upgrade_group = 'peer_deal_no_cost'
		then (f.upgrade_annual_oop_actual/f.students_upgrade_actual)*f.students_upgrade_only_extrap
	else sum((students_upgrade_only_extrap*percent_upgraded_to_meet)*p80_annual_oop_increase_per_student) 
end as upgrade_annual_oop_extrap_only,
case 
	when upgrade_group = 'peer_deal_no_cost'
		then (f.upgrade_annual_oop_actual/f.students_upgrade_actual)*f.students_upgrade_extrap
	else sum((students_upgrade_only_extrap*percent_upgraded_to_meet)*p80_annual_oop_increase_per_student) + f.upgrade_annual_oop_actual 
end as upgrade_annual_oop_total,

--Annual Cost --- extrapolating costs differently for districts with peer deals
f.upgrade_annual_cost_actual,
case 
	when upgrade_group = 'peer_deal_no_cost'
		then (f.upgrade_annual_cost_actual/f.students_upgrade_actual)*f.students_upgrade_only_extrap
	else sum((students_upgrade_only_extrap*percent_upgraded_to_meet)*p80_annual_cost_increase_per_student) 
end as upgrade_annual_cost_extrap_only,
case 
	when upgrade_group = 'peer_deal_no_cost'
		then (f.upgrade_annual_cost_actual/f.students_upgrade_actual)*f.students_upgrade_extrap
	else sum((students_upgrade_only_extrap*percent_upgraded_to_meet)*p80_annual_cost_increase_per_student) + f.upgrade_annual_cost_actual 
end as upgrade_annual_cost_total,
f.districts_meeting_after_upgrade_p,
f.students_meeting_after_upgrade_p,
f.districts_meeting_after_upgrade_extrap,
f.students_meeting_after_upgrade_extrap,
f.districts_not_meeting_after_upgrade_p,
f.students_not_meeting_after_upgrade_p,
f.districts_not_meeting_after_upgrade_extrap,
f.students_not_meeting_after_upgrade_extrap,
f.districts_pop,
f.students_pop,
f.not_meeting_2019_districts_before_p,
f.not_meeting_2019_students_before_p,
f.meeting_2019_districts_before_p,
f.meeting_2019_students_before_p,
f.districts_not_meeting_before_extrap,
f.students_not_meeting_before_extrap,
f.districts_meeting_before_extrap,
f.students_meeting_before_extrap


from final_before_cost f 

join newly_meeting_cost c
on c.year > 0 


group by 
f.districts_pop,
f.students_pop,
f.not_meeting_2019_districts_before_p,
f.not_meeting_2019_students_before_p,
f.meeting_2019_districts_before_p,
f.meeting_2019_students_before_p,
f.districts_not_meeting_before_extrap,
f.students_not_meeting_before_extrap,
f.districts_meeting_before_extrap,
f.students_meeting_before_extrap,
f.upgrade_group,
f.districts_upgrade_p,
f.students_upgrade_p,
f.districts_upgrade_actual,
f.students_upgrade_actual,
f.districts_upgrade_extrap,
f.students_upgrade_extrap,
f.students_upgrade_only_extrap,
f.districts_meeting_after_upgrade_p,
f.students_meeting_after_upgrade_p,
f.districts_meeting_after_upgrade_extrap,
f.students_meeting_after_upgrade_extrap,
f.districts_not_meeting_after_upgrade_p,
f.students_not_meeting_after_upgrade_p,
f.districts_not_meeting_after_upgrade_extrap,
f.students_not_meeting_after_upgrade_extrap,
f.upgrade_mrc_actual,
f.upgrade_annual_oop_actual,
f.upgrade_annual_cost_actual