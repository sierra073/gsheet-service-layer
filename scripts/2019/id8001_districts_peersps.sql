SELECT 
--district in question
dpr.district_id AS district_id,
d.state_code AS district_state_code,
--entities under the district's jurisdiction (schools/the district)
rd.recipient_id as entity_id,
coalesce(s.name,d.name) AS entity_name,
coalesce(s.latitude,d.latitude) as entity_latitude,
coalesce(s.longitude,d.longitude) as entity_longitude,
--service providers that provide an internet line item to a peer to the district
array_agg(distinct li2.spin) as peersp_spin,
array_agg(distinct li2.doing_business_as) as peersp_doing_business_as,
array_agg(distinct li2.parent_name) as peersp_parent_name

--peers lookup
FROM ps.districts_peers_ranks  dpr

--district info
JOIN ps.districts  d
on  d.district_id = dpr.district_id 
AND  d.funding_year = dpr.funding_year
join ps.districts_bw_cost   dbc
on dbc.district_id = dpr.district_id 
AND dbc.funding_year = dpr.funding_year
join ps.districts_fit_for_analysis  dffa
on  dffa.district_id = d.district_id
AND dffa.funding_year = d.funding_year
join ps.recipients_districts_lkp rd
on d.district_id = rd.district_id
and d.funding_year = rd.funding_year
left join ps.schools s
on rd.recipient_id = s.school_id
and rd.funding_year = s.funding_year

--peer info
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

WHERE
dpr.funding_year = 2019 and
dffa.fit_for_ia_cost = TRUE and
dffa2.fit_for_ia_cost = TRUE and
d.in_universe = true AND
d2.in_universe = true AND
dbc.meeting_2018_goal_no_oversub = false and
dpr.peer_bw_over_primary_district_bw_2018_goal=true

group by 1,2,3,4,5,6