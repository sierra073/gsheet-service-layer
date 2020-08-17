with district_peer_agg as (
	select district_id,
	funding_year,
	array_agg(distinct peer_service_provider) as deal_provider_array

	from ps.districts_peers_ranks
	where peer_bw_over_primary_district_bw_2018_goal = True
	and same_primary_sp = True

	group by district_id,
	funding_year
),

categorize_line_item_deals as (select funding_year,
				district_id,
				deal
				from ps.peer_deal_line_items dli

				group by funding_year,
				district_id,
				deal

				having count(line_item_id) = sum(case
						when current_provider = true
							then 1 else 0 end)
	),

line_item_deal_array as (select dli.funding_year,
	dli.district_id,
	array_agg(distinct parent_name) as deal_provider_array

	from ps.peer_deal_line_items dli

	inner join categorize_line_item_deals
	on categorize_line_item_deals.funding_year = dli.funding_year
	and categorize_line_item_deals.district_id = dli.district_id
	and categorize_line_item_deals.deal = dli.deal

	group by dli.funding_year,
	dli.district_id
)

select dd.district_id,
dd.name,
dd.state_code,
dd.num_students,
u.path_to_meet_2018_goal_group = 'No Cost Peer Deal' as spiya_or_peer_deal,
case 
	when u.current_provider_deal = True 
		then True
	else False
end as current_provider_deal,
case 
	when s.peer_deal_type = 'line_items'
		then lid.deal_provider_array
	when s.peer_deal_type = 'district_peers'
		then dp.deal_provider_array
end as deal_current_provider_array

from ps.districts dd

inner join ps.districts_fit_for_analysis fit
on dd.district_id = fit.district_id
and dd.funding_year = fit.funding_year

inner join ps.districts_bw_cost bc 
on dd.district_id = bc.district_id
and dd.funding_year = bc.funding_year

left join ps.districts_lines dl 
on dd.district_id = dl.district_id
and dd.funding_year = dl.funding_year

inner join ps.districts_upgrades u 
on dd.district_id = u.district_id
and dd.funding_year = u.funding_year

inner join ps.states_static s 
on dd.state_code = s.state_code

left join district_peer_agg dp 
on dp.district_id = dd.district_id
and dp.funding_year = dd.funding_year

left join line_item_deal_array lid 
on lid.district_id = dd.district_id
and lid.funding_year = dd.funding_year

where dd.funding_year = 2019
and dd.district_type = 'Traditional'
and dd.in_universe = True 
and fit.fit_for_ia = True
and fit.fit_for_ia_cost = True 
and bc.ia_monthly_cost_total > 0
and bc.meeting_2018_goal_oversub = False
and s.state_network_natl_analysis = False
