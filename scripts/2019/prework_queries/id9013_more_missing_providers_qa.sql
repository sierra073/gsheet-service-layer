with old_peer_deals_given_geotel_id8001 as (
SELECT distinct
dpr.district_id AS district_id,
d.state_code AS district_state_code,
li2.parent_name as peersp_parent_name

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

WHERE
dpr.funding_year = 2019 and
dffa.fit_for_ia_cost = TRUE and
dffa2.fit_for_ia_cost = TRUE and
d.in_universe = true AND
d2.in_universe = true AND
dbc.meeting_2018_goal_no_oversub = false and
dpr.peer_bw_over_primary_district_bw_2018_goal=true
)

/* districts WITHOUT NEW SP_IYA PEER DEAL */
select distinct
d.district_id,
case when op.peersp_parent_name is not null
  then op.peersp_parent_name
else 'No Old Peer Deal' end as old_peersp_parent_name

from ps.districts d

join ps.districts_fit_for_analysis dfit
on d.district_id = dfit.district_id
and d.funding_year = dfit.funding_year

join ps.districts_bw_cost dbc
on d.district_id = dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_upgrades du
on d.district_id = du.district_id
and d.funding_year = du.funding_year

left join old_peer_deals_given_geotel_id8001 op
on d.district_id = op.district_id

where d.in_universe = true
and d.district_type = 'Traditional'
and d.funding_year = 2019
and dfit.fit_for_ia = true
and dfit.fit_for_ia_cost = true
and dbc.meeting_2018_goal_oversub = false
and path_to_meet_2018_goal_group != 'No Cost Peer Deal'
