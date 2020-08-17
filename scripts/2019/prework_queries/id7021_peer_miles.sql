SELECT distinct on (dpr.district_id, li2.parent_name)
dpr.district_id AS district_id,
d.state_code AS district_state_code,
UPPER(li2.parent_name) as peersp_parent_name,
ST_Distance(geography(d.geom),geography(d2.geom))*0.00062137 as distance_from_peer,
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

order by dpr.district_id, li2.parent_name, distance_from_peer asc 