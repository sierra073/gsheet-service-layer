with no_peer_deal as (
	select 
	dd.funding_year,
	dd.state_code,
	dd.district_id,
	dd.num_students,
	bc.ia_bw_mbps_total as old_total_bw,
	bc.ia_monthly_cost_total as old_total_cost,
	bc.ia_bandwidth_per_student_kbps as old_bw_per_student,
	bc.projected_bw_fy2018

	from ps.districts dd

	inner join ps.districts_fit_for_analysis fit
	on dd.district_id = fit.district_id
	and dd.funding_year = fit.funding_year

	inner join ps.districts_bw_cost bc 
	on dd.district_id = bc.district_id
	and dd.funding_year = bc.funding_year

	inner join ps.districts_upgrades u 
	on dd.district_id = u.district_id
	and dd.funding_year = u.funding_year

	inner join ps.states_static s 
	on dd.state_code = s.state_code

	where dd.funding_year = 2019
	and dd.district_type = 'Traditional'
	and dd.in_universe = True 
	and fit.fit_for_ia = True
	and fit.fit_for_ia_cost = True 
	and bc.ia_monthly_cost_total > 0
	and bc.meeting_2018_goal_oversub = False
	and s.peer_deal_type = 'district_peers'
	and u.district_peers_path_to_meet_2018_group != 'No Cost Peer Deal'
)

select distinct on (no_peer_deal.district_id, no_peer_deal.funding_year)
no_peer_deal.*,
p.peer_id is not null as best_deal,
p.peer_id,
p.peer_ia_bw_mbps_total as new_total_bw,
(p.peer_ia_bw_mbps_total*1000)/no_peer_deal.num_students as new_bw_per_student,
case 
	when p.district_id is not null
		then p.peer_ia_bw_mbps_total - no_peer_deal.old_total_bw
end as total_bw_change,
case 
	when p.district_id is not null
		then (p.peer_ia_bw_mbps_total - no_peer_deal.old_total_bw)/no_peer_deal.old_total_bw
end as total_bw_percent_change

from no_peer_deal

left join ps.districts_peers_ranks p 
on p.funding_year = no_peer_deal.funding_year
and p.district_id = no_peer_deal.district_id

order by no_peer_deal.district_id, no_peer_deal.funding_year, p.peer_ia_bw_mbps_total desc 