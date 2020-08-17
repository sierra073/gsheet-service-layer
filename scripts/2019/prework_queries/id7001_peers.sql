--copied logic from districts_peers_ranks but removed cost restrictions, unnecessary fields
select peers.district_id,
	dd.num_students,
	case 
		when extract(month from  dl.most_recent_ia_contract_end_date) <= 6
			then extract(year from dl.most_recent_ia_contract_end_date)
		when extract(month from dl.most_recent_ia_contract_end_date) > 6
			then extract(year from dl.most_recent_ia_contract_end_date) + 1 
		else dd.funding_year + 1
	end as primary_new_contract_start_date,
	dl.most_recent_ia_contract_end_date is null as primary_contract_date_null,
	round(bc.ia_monthly_cost_total,2) as primary_ia_monthly_cost_total,
	peers.peer_id,
	(dd.state_code = dd2.state_code) as same_state,
	dd2.state_code as match_state,
	case 
		when sp.primary_sp is null
			then false 
		else sp.primary_sp = sp2.primary_sp 
	end as same_primary_sp,
	sp2.primary_sp as match_service_provider,
	bc2.ia_bw_mbps_total as match_ia_bw_mbps_total,
	dl2.all_ia_connectcat as match_all_ia_connectcat,
	round(bc2.ia_monthly_cost_total,2) as match_ia_monthly_cost_total,
	round(bc2.ia_monthly_cost_per_mbps,2) as match_ia_monthly_cost_per_mbps

	from dwh.districts_peers_lkp peers

	join ps.districts dd
	on peers.district_id = dd.district_id
	AND peers.funding_year = dd.funding_year

	join ps.districts dd2
	on peers.peer_id = dd2.district_id
	AND peers.funding_year = dd2.funding_year

	join ps.districts_lines dl 
	on dl.district_id = dd.district_id
	and dl.funding_year = dd.funding_year

	join ps.districts_lines dl2
	on dd2.district_id = dl2.district_id
	AND dd2.funding_year = dl2.funding_year

	join ps.districts_fit_for_analysis fit
	on peers.district_id = fit.district_id
	AND peers.funding_year = fit.funding_year

	join ps.districts_fit_for_analysis fit2
	on peers.peer_id = fit2.district_id
	AND peers.funding_year = fit2.funding_year

	join ps.districts_bw_cost bc
	on peers.district_id = bc.district_id
	AND peers.funding_year = bc.funding_year

	join ps.districts_bw_cost bc2
	on peers.peer_id = bc2.district_id
	AND peers.funding_year = bc2.funding_year

	join ps.districts_sp_assignments sp
	on peers.district_id = sp.district_id
	and peers.funding_year = sp.funding_year

	join ps.districts_sp_assignments sp2
	on peers.peer_id = sp2.district_id
	and peers.funding_year = sp2.funding_year

	where dd.funding_year = 2019
	and peers.district_id != peers.peer_id
	and dd.in_universe = TRUE
	and dd2.in_universe = TRUE
	and dd.district_type = 'Traditional'
	and dd2.district_type = 'Traditional'
	and fit.fit_for_ia = TRUE 
	and fit2.fit_for_ia = TRUE
	and fit.fit_for_ia_cost = TRUE
	and fit2.fit_for_ia_cost = TRUE
	and bc2.ia_monthly_cost_total != 0
	and dl2.all_ia_connectcat ilike '%Lit Fiber%' 
	and not(lower(sp2.primary_sp) ilike '%owned%')
	and lower(sp2.primary_sp) not in ('unknown','n/a','')
	
	and bc.projected_bw_fy2018 <= bc2.ia_bw_mbps_total
	and bc.meeting_2018_goal_oversub = False

