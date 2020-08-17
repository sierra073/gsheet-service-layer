with peer_providers as (
	SELECT dpr.district_id AS district_id,
	d.state_code AS district_state_code,
	li2.parent_name as peersp_parent_name,
	d.geom as district_geom,
	dpr.funding_year,
	ss.state_network as district_state_network

	FROM ps.districts_peers_ranks  dpr

	JOIN ps.districts  d
	on  d.district_id = dpr.district_id
	AND  d.funding_year = dpr.funding_year
	join ps.districts_bw_cost   dbc
	on dbc.district_id = dpr.district_id
	AND dbc.funding_year = dpr.funding_year
	join ps.districts_fit_for_analysis  dffa
	on  dffa.district_id = d.district_id
	AND dffa.funding_year = d.funding_year

	JOIN ps.districts  d2
	on d2.district_id = dpr.peer_id
	AND d2.funding_year = dpr.funding_year

	join ps.districts_fit_for_analysis dffa2
	on dffa2.district_id = d2.district_id
	and dffa2.funding_year = d2.funding_year

	join ps.districts_line_items dli2
	on dli2.district_id = d2.district_id
	and dli2.funding_year = d2.funding_year
	and dli2.purpose in ('internet', 'isp', 'upstream')

	join ps.line_items li2
	on dli2.line_item_id = li2.line_item_id

	join ps.districts_upgrades du 
	on dpr.district_id = du.district_id
	and dpr.funding_year = du.funding_year

	join ps.states_static ss
	on ss.state_code = d.state_code 

	where dpr.funding_year = 2019 
	and dffa.fit_for_ia_cost = TRUE 
	and dffa2.fit_for_ia_cost = TRUE
	and d.in_universe = true
	and d2.in_universe = true
	and d.district_type = 'Traditional'
	and d2.district_type = 'Traditional'
	and dbc.meeting_2018_goal_oversub = false
	and dpr.peer_bw_over_primary_district_bw_2018_goal=true
	and du.path_to_meet_2018_goal_group != 'No Cost Peer Deal'

	group by dpr.district_id,
	d.state_code,
	li2.parent_name,
	d.geom,
	dpr.funding_year,
	ss.state_network
),

provider_all_districts as (select pp.district_id,
	pp.district_state_code,
	pp.peersp_parent_name,
	pp.district_geom,
	pp.district_state_network,
	d2.geom,
	pp.district_id = d2.district_id as already_receives_peer_deal_provider

	from peer_providers pp 

	join ps.line_items li 
	on pp.funding_year = li.funding_year
	and pp.peersp_parent_name = li.parent_name

	join ps.districts_line_items dli 
	on li.line_item_id = dli.line_item_id

	join ps.districts d2 
	on dli.district_id = d2.district_id
	and dli.funding_year = d2.funding_year

	where d2.in_universe = True 
	and d2.district_type = 'Traditional'
	-- too many districts, have to limit it by state
	and d2.state_code = pp.district_state_code

	group by pp.district_id,
	pp.district_state_code,
	pp.peersp_parent_name,
	pp.district_geom,
	pp.district_state_network,
	d2.geom,
	d2.district_id
)


select distinct on (district_id, peersp_parent_name)
district_id,
district_state_code,
district_state_network,
UPPER(peersp_parent_name) as peersp_parent_name,
already_receives_peer_deal_provider,
ST_Distance(geography(district_geom),geography(geom))*0.00062137 as distance_from_closet_district_w_provider

from provider_all_districts

order by district_id, peersp_parent_name, distance_from_closet_district_w_provider asc