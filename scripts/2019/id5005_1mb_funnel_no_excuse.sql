-- all not meeting 1mb districts with same sp deals and contract expiring
---combined
with year_meeting as (
SELECT dd.district_id,
  CASE
    WHEN bw15.meeting_2018_goal_oversub = true AND ff15.fit_for_ia = true THEN 2015
    WHEN bw16.meeting_2018_goal_oversub = true AND ff16.fit_for_ia = true THEN 2016
    WHEN bw17.meeting_2018_goal_oversub = true AND ff17.fit_for_ia = true THEN 2017
    WHEN bw18.meeting_2018_goal_oversub = true AND ff18.fit_for_ia = true THEN 2018
    WHEN bw19.meeting_2018_goal_oversub = true AND ff19.fit_for_ia = true THEN 2019
    END AS year_meeting_1mbps

FROM ps.districts dd

LEFT JOIN ps.districts_bw_cost bw15
ON dd.district_id = bw15.district_id
AND bw15.funding_year = 2015

LEFT JOIN ps.districts_fit_for_analysis ff15
ON bw15.district_id = ff15.district_id
AND bw15.funding_year = ff15.funding_year

LEFT JOIN ps.districts_bw_cost bw16
ON dd.district_id = bw16.district_id
AND bw16.funding_year = 2016

LEFT JOIN ps.districts_fit_for_analysis ff16
ON bw16.district_id = ff16.district_id
AND bw16.funding_year = ff16.funding_year

LEFT JOIN ps.districts_bw_cost bw17
ON dd.district_id = bw17.district_id
AND bw17.funding_year = 2017

LEFT JOIN ps.districts_fit_for_analysis ff17
ON bw17.district_id = ff17.district_id
AND bw17.funding_year = ff17.funding_year

LEFT JOIN ps.districts_bw_cost bw18
ON dd.district_id = bw18.district_id
AND bw18.funding_year = 2018

LEFT JOIN ps.districts_fit_for_analysis ff18
ON bw18.district_id = ff18.district_id
AND bw18.funding_year = ff18.funding_year

LEFT JOIN ps.districts_bw_cost bw19
ON dd.district_id = bw19.district_id
AND bw19.funding_year = dd.funding_year

LEFT JOIN ps.districts_fit_for_analysis ff19
ON bw19.district_id = ff19.district_id
AND bw19.funding_year = ff19.funding_year

WHERE dd.funding_year = 2019
and dd.in_universe = true
and dd.district_type = 'Traditional'
and bw18.meeting_2018_goal_oversub = true
),

first_table_19 as (
  SELECT
  dd.district_id,
  dd.funding_year,
  dd.state_code,
  CASE WHEN ee.year_meeting_1mbps IS NOT NULL THEN dee.num_students
  ELSE dd.num_students END as num_students,
  CASE
    WHEN bb.meeting_2018_goal_oversub = false AND ff.fit_for_ia = true THEN 'not_meeting'
    WHEN (ee.year_meeting_1mbps IS NOT NULL OR bb.meeting_2018_goal_oversub = true) AND ff.fit_for_ia = true THEN 'meeting'
  ELSE 'unknown' END as meeting_2018,
  bb.ia_bw_mbps_total as bw19,
  bb.ia_monthly_cost_total as cost19,
  ff.fit_for_ia,
  ee.year_meeting_1mbps,
  CASE
    WHEN li.most_recent_primary_ia_contract_end_date <= '2020-06-30' THEN 'expiring'
    WHEN (li.most_recent_primary_ia_contract_end_date > '2020-06-30' OR li.most_recent_primary_ia_contract_end_date ISNULL) THEN 'not expiring'
    ELSE 'unknown' END as having_contract_expiring

  FROM ps.districts dd

  JOIN ps.districts_bw_cost bb
  ON dd.district_id = bb.district_id
  AND dd.funding_year = bb.funding_year

  JOIN ps.districts_fit_for_analysis ff
  ON ff.district_id = dd.district_id
  AND ff.funding_year = dd.funding_year

  JOIN ps.districts_lines li
  on li.district_id = dd.district_id
  and li.funding_year = dd.funding_year

  LEFT JOIN year_meeting ee
  on ee.district_id = dd.district_id

  LEFT JOIN ps.districts dee
  on ee.district_id = dee.district_id
  and ee.year_meeting_1mbps = dee.funding_year

  WHERE dd.funding_year = 2019
  and dd.in_universe = true
  and dd.district_type = 'Traditional'
),

first_table_18 as (
 SELECT
  dd.district_id,
  dd.funding_year,
  dd.state_code,
  CASE WHEN ee.year_meeting_1mbps IS NOT NULL THEN dee.num_students
  ELSE dd.num_students END as num_students,
  CASE
    WHEN bb.meeting_2018_goal_oversub = false AND ff.fit_for_ia = true THEN 'not_meeting'
    WHEN (ee.year_meeting_1mbps IS NOT NULL OR bb.meeting_2018_goal_oversub = true) AND ff.fit_for_ia = true THEN 'meeting'
  ELSE 'unknown' END as meeting_2018,
  bb.ia_bw_mbps_total as bw18,
  bb.ia_monthly_cost_total as cost18,
  ff.fit_for_ia,
  ee.year_meeting_1mbps,
  CASE
    WHEN li.most_recent_primary_ia_contract_end_date <= '2019-06-30' THEN 'expiring'
    WHEN (li.most_recent_primary_ia_contract_end_date > '2019-06-30' OR li.most_recent_primary_ia_contract_end_date ISNULL) THEN 'not expiring'
    ELSE 'unknown' END as having_contract_expiring

  from ps.districts dd

  join ps.districts_bw_cost bb
  on dd.district_id = bb.district_id
  and dd.funding_year = bb.funding_year

  join ps.districts_fit_for_analysis ff
  on ff.district_id = dd.district_id
  and ff.funding_year = dd.funding_year

  JOIN ps.districts_lines li
  on li.district_id = dd.district_id
  and li.funding_year = dd.funding_year

  LEFT JOIN year_meeting ee
  on ee.district_id = dd.district_id

  LEFT JOIN ps.districts dee
  on ee.district_id = dee.district_id
  and ee.year_meeting_1mbps = dee.funding_year

  WHERE dd.funding_year = 2018
  and dd.in_universe = true
  and dd.district_type = 'Traditional'
),

second_table_19 as (

SELECT
  ff.district_id,
  ff.meeting_2018,
  ff.year_meeting_1mbps,
  ff.having_contract_expiring,
  count(distinct pr.district_id) FILTER (WHERE pr.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS num_peer_deals,
  count(distinct pr.district_id) FILTER (WHERE pr.current_provider_deal = true AND pr.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS same_sp_deal,
  dd.state_code,
  dd.size,
  dd.locale,
  dd.num_students,
  dd.num_campuses,
  dd.consortium_affiliation,
  CASE when dd.consortium_affiliation is not null then true else false end as consortium_tf,
  sp.primary_sp,
  sp.primary_sp_bandwidth,
  sp.service_provider_id,
  dot.c1_barriers_to_upgrade,
  ff.num_students

FROM first_table_19 ff

JOIN ps.districts_bw_cost dbw
ON ff.district_id = dbw.district_id

JOIN ps.districts dd
ON dd.district_id = dbw.district_id
AND dd.funding_year = dbw.funding_year

left JOIN ps.districts_sp_assignments sp
ON dbw.district_id = sp.district_id
AND dbw.funding_year = sp.funding_year

LEFT JOIN ps.districts_upgrades pr
ON dbw.district_id = pr.district_id
AND dbw.funding_year = pr.funding_year

LEFT JOIN ps.districts_outreach dot
ON dot.district_id = dd.district_id
AND dot.funding_year = dd.funding_year

WHERE ff.fit_for_ia = true
AND dbw.funding_year = 2019
GROUP BY ff.district_id,
  ff.meeting_2018,
  ff.year_meeting_1mbps,
  ff.having_contract_expiring,
  dd.state_code,
  dd.size,
  dd.locale,
  dd.num_students,
  dd.num_campuses,
  dd.consortium_affiliation,
  sp.primary_sp,
  sp.primary_sp_bandwidth,
  sp.service_provider_id,
  dot.c1_barriers_to_upgrade,
  ff.num_students
),

second_table_18 as (

SELECT
  ff.district_id,
  ff.meeting_2018,
  ff.year_meeting_1mbps,
  ff.having_contract_expiring,
  count(distinct pr.district_id) FILTER (WHERE pr.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS num_peer_deals,
  count(distinct pr.district_id) FILTER (WHERE pr.current_provider_deal = true AND pr.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS same_sp_deal,
  dd.state_code,
  dd.size,
  dd.locale,
  dd.num_students,
  dd.num_campuses,
  dd.consortium_affiliation,
  CASE when dd.consortium_affiliation is not null then true else false end as consortium_tf,
  sp.primary_sp,
  sp.primary_sp_bandwidth,
  sp.service_provider_id,
  dot.c1_barriers_to_upgrade,
  ff.num_students

FROM first_table_18 ff

JOIN ps.districts_bw_cost dbw
ON ff.district_id = dbw.district_id
and ff.funding_year = dbw.funding_year

JOIN ps.districts dd
ON dd.district_id = dbw.district_id
AND dd.funding_year = dbw.funding_year

JOIN ps.districts_sp_assignments sp
ON dbw.district_id = sp.district_id
AND dbw.funding_year = sp.funding_year

LEFT JOIN ps.districts_upgrades pr
ON dbw.district_id = pr.district_id
AND dbw.funding_year = pr.funding_year

LEFT JOIN ps.districts_outreach dot
ON dot.district_id = dd.district_id
AND dot.funding_year = dd.funding_year

WHERE ff.meeting_2018 = 'not_meeting'
AND ff.fit_for_ia = true
AND dbw.funding_year = 2018
GROUP BY ff.district_id,
  ff.meeting_2018,
  ff.year_meeting_1mbps,
  ff.having_contract_expiring,
  dd.state_code,
  dd.size,
  dd.locale,
  dd.num_students,
  dd.num_campuses,
  dd.consortium_affiliation,
  sp.primary_sp,
  sp.primary_sp_bandwidth,
  sp.service_provider_id,
  dot.c1_barriers_to_upgrade,
  ff.num_students
),

counts as (
  select
    'students' as version,
    sum(f18.num_students) as total_2018_no_excuse,
    sum(f18.num_students) FILTER (WHERE s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) as meeting_2019,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and s19.primary_sp = s18.primary_sp) as meeting_2019_same_sp,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and (s19.primary_sp <> s18.primary_sp or s19.primary_sp is null)) as meeting_2019_diff_sp,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and s19.primary_sp = s18.primary_sp and f19.cost19 <= f18.cost18) as meeting_2019_pdeal_same_sp,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and (s19.primary_sp <> s18.primary_sp or s19.primary_sp is null) and f19.cost19 <= f18.cost18) as meeting_2019_pdeal_diff_sp,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and s19.primary_sp = s18.primary_sp and f19.cost19 > f18.cost18) as meeting_2019_paid_more_same_sp,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and (s19.primary_sp <> s18.primary_sp or s19.primary_sp is null) and f19.cost19 > f18.cost18) as meeting_2019_paid_more_diff_sp,
    sum(f18.num_students) FILTER (WHERE s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) as total_still_not_meeting_2019,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 > f18.bw18) as upgraded_still_not_meeting,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 <= f18.bw18) as not_upgraded_still_not_meeting,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 > f18.bw18 and s19.primary_sp = s18.primary_sp) as upgraded_not_meeting_same_sp,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 > f18.bw18 and (s19.primary_sp <> s18.primary_sp or s19.primary_sp is null)) as upgraded_not_meeting_diff_sp,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 <= f18.bw18 AND (s19.num_peer_deals <> 0 AND s19.same_sp_deal <> 0) AND (s19.having_contract_expiring = 'expiring')) no_upgrade_no_excuse,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 <= f18.bw18 AND (s19.num_peer_deals = 0 OR s19.same_sp_deal = 0 OR s19.having_contract_expiring = 'not expiring')) no_upgrade_yes_excuse,
    sum(f18.num_students) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and s19.having_contract_expiring = 'not expiring') as not_meeting_not_expiring_2019

  from first_table_18 f18

  LEFT JOIN second_table_18 s18
  on f18.district_id = s18.district_id

  LEFT JOIN first_table_19 f19
  on f19.district_id = f18.district_id

  LEFT JOIN second_table_19 s19
  on s19.district_id = s18.district_id

  LEFT join ps.districts_bw_cost d
  on d.district_id = f18.district_id
  and d.funding_year = 2019

  LEFT JOIN ps.states_static sn
  on f18.state_code = sn.state_code

  WHERE s18.having_contract_expiring = 'expiring'
  AND s18.meeting_2018 = 'not_meeting'
  AND s18.num_peer_deals > 0
  AND s18.same_sp_deal > 0
  AND f18.fit_for_ia = true
  AND f19.fit_for_ia = true
  AND sn.state_network_natl_analysis = False

  UNION ALL

select
    'districts' as version,
    count(distinct f18.district_id) as total_2018_no_excuse,
    count(distinct f18.district_id) FILTER (WHERE s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) as meeting_2019,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and s19.primary_sp = s18.primary_sp) as meeting_2019_same_sp,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and (s19.primary_sp <> s18.primary_sp or s19.primary_sp is null)) as meeting_2019_diff_sp,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and s19.primary_sp = s18.primary_sp and f19.cost19 <= f18.cost18) as meeting_2019_pdeal_same_sp,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and (s19.primary_sp <> s18.primary_sp or s19.primary_sp is null) and f19.cost19 <= f18.cost18) as meeting_2019_pdeal_diff_sp,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and s19.primary_sp = s18.primary_sp and f19.cost19 > f18.cost18) as meeting_2019_paid_more_same_sp,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true) and (s19.primary_sp <> s18.primary_sp or s19.primary_sp is null) and f19.cost19 > f18.cost18) as meeting_2019_paid_more_diff_sp,
    count(distinct f18.district_id) FILTER (WHERE s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) as total_still_not_meeting_2019,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 > f18.bw18) as upgraded_still_not_meeting,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 <= f18.bw18) as not_upgraded_still_not_meeting,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 > f18.bw18 and s19.primary_sp = s18.primary_sp) as upgraded_not_meeting_same_sp,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 > f18.bw18 and (s19.primary_sp <> s18.primary_sp or s19.primary_sp is null)) as upgraded_not_meeting_diff_sp,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 <= f18.bw18 AND (s19.num_peer_deals <> 0 AND s19.same_sp_deal <> 0) AND (s19.having_contract_expiring = 'expiring')) no_upgrade_no_excuse,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and f19.bw19 <= f18.bw18 AND (s19.num_peer_deals = 0 OR s19.same_sp_deal = 0 OR s19.having_contract_expiring = 'not expiring')) no_upgrade_yes_excuse,
    count(distinct f18.district_id) FILTER (WHERE (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false) and s19.having_contract_expiring = 'not expiring') as not_meeting_not_expiring_2019

  from first_table_18 f18

  LEFT JOIN second_table_18 s18
  on f18.district_id = s18.district_id

  LEFT JOIN first_table_19 f19
  on f19.district_id = f18.district_id

  LEFT JOIN second_table_19 s19
  on s19.district_id = s18.district_id

  LEFT join ps.districts_bw_cost d
  on d.district_id = f18.district_id
  and d.funding_year = 2019

  LEFT JOIN ps.states_static sn
  on f18.state_code = sn.state_code

  WHERE s18.having_contract_expiring = 'expiring'
  AND s18.meeting_2018 = 'not_meeting'
  AND s18.num_peer_deals > 0
  AND s18.same_sp_deal > 0
  AND f18.fit_for_ia = true
  AND f19.fit_for_ia = true
  AND sn.state_network_natl_analysis = False
),

extrapolate_deals as (
  SELECT
    version,
    (2-(meeting_2019_pdeal_same_sp + meeting_2019_pdeal_diff_sp + meeting_2019_paid_more_same_sp + meeting_2019_paid_more_diff_sp)::numeric/meeting_2019)
    as meeting_extrap_perc,
    (2-(upgraded_not_meeting_same_sp + upgraded_not_meeting_diff_sp)::numeric/upgraded_still_not_meeting)
    as upg_not_meeting_extrap_perc,
    (2-(no_upgrade_no_excuse + no_upgrade_yes_excuse)/not_upgraded_still_not_meeting)
    as not_upg_not_meeting_extrap_perc

    from counts
)

SELECT
  c.version,
  total_2018_no_excuse,
  meeting_2019,
  total_still_not_meeting_2019,
  round((meeting_2019_pdeal_same_sp + meeting_2019_pdeal_diff_sp)::numeric*meeting_extrap_perc) as meeting_pdeals_exp,
  round((meeting_2019_paid_more_same_sp + meeting_2019_paid_more_diff_sp)::numeric*meeting_extrap_perc) as meeting_paid_more_exp,
  upgraded_still_not_meeting,
  not_upgraded_still_not_meeting,
  round(meeting_2019_pdeal_same_sp*meeting_extrap_perc) as meeting_pdeal_same_sp_exp,
  round(meeting_2019_pdeal_diff_sp*meeting_extrap_perc) as meeting_pdeal_diff_sp_exp,
  round(meeting_2019_paid_more_same_sp*meeting_extrap_perc) as meeting_paid_more_same_sp_exp,
  round(meeting_2019_paid_more_diff_sp*meeting_extrap_perc) as meeting_paid_more_diff_sp_exp,
  round(upgraded_not_meeting_same_sp*upg_not_meeting_extrap_perc) as upgraded_not_meeting_same_sp_exp,
  round(upgraded_not_meeting_diff_sp*upg_not_meeting_extrap_perc) as upgraded_not_meeting_diff_sp_exp,
  round(no_upgrade_no_excuse*not_upg_not_meeting_extrap_perc) as no_upgrade_no_excuse_exp,
  round(no_upgrade_yes_excuse*not_upg_not_meeting_extrap_perc) as no_upgrade_yes_excuse_exp,
  round(not_meeting_not_expiring_2019*not_upg_not_meeting_extrap_perc) as not_meeting_not_expiring_2019_exp

  from counts c

  join extrapolate_deals ex
  on ex.version = c.version
