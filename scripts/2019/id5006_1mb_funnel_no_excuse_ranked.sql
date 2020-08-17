
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

expiration as (
SELECT dd.district_id,
dd.funding_year,
yy.year_meeting_1mbps,
CASE
  WHEN yy.year_meeting_1mbps = 2016 AND dl15.most_recent_primary_ia_contract_end_date >= '2016-06-30' THEN 'expiring'
  WHEN yy.year_meeting_1mbps = 2017 AND dl16.most_recent_primary_ia_contract_end_date >= '2017-06-30' THEN 'expiring'
  WHEN yy.year_meeting_1mbps = 2018 AND dl17.most_recent_primary_ia_contract_end_date >= '2018-06-30' THEN 'expiring'
  WHEN yy.year_meeting_1mbps = 2019 AND dl18.most_recent_primary_ia_contract_end_date >= '2019-06-30' THEN 'expiring'
  ELSE 'not expiring'
  END AS contract_expiring

FROM ps.districts dd

JOIN year_meeting yy
on dd.district_id = yy.district_id

LEFT JOIN ps.districts_lines dl15
ON dd.district_id = dl15.district_id
and dl15.funding_year = 2015

LEFT JOIN ps.districts_lines dl16
ON dd.district_id = dl16.district_id
and dl16.funding_year = 2016

LEFT JOIN ps.districts_lines dl17
ON dd.district_id = dl17.district_id
and dl17.funding_year = 2017

LEFT JOIN ps.districts_lines dl18
ON dd.district_id = dl18.district_id
and dl18.funding_year = 2018

LEFT JOIN ps.districts_lines dl19
ON dd.district_id = dl19.district_id
and dl19.funding_year = 2019

where dd.funding_year in (2018, 2019)
),

first_table_19 as (
  SELECT
  dd.district_id,
  dd.funding_year,
  CASE WHEN bb.meeting_2018_goal_oversub = false AND ff.fit_for_ia = true THEN 'not_meeting'
  WHEN (ee.year_meeting_1mbps IS NOT NULL OR bb.meeting_2018_goal_oversub = true) THEN 'meeting'
  ELSE 'unknown' END as meeting_2018,
  bb.ia_bw_mbps_total as bw19,
  bb.ia_monthly_cost_total as cost19,
  ff.fit_for_ia,
  ee.year_meeting_1mbps,
  CASE
    WHEN ee.contract_expiring IS NOT NULL THEN ee.contract_expiring
    WHEN li.most_recent_primary_ia_contract_end_date <= '2020-06-30' THEN 'expiring'
    WHEN (li.most_recent_primary_ia_contract_end_date > '2020-06-30' OR li.most_recent_primary_ia_contract_end_date ISNULL) THEN 'not expiring'
    ELSE 'unknown' END as having_contract_expiring,
  li.consortia_applied_ia_lines

  from ps.districts dd

  join ps.districts_bw_cost bb
  on dd.district_id = bb.district_id
  and dd.funding_year = bb.funding_year

  join ps.districts_fit_for_analysis ff
  on ff.district_id = dd.district_id
  and ff.funding_year = dd.funding_year

  left JOIN ps.districts_lines li
  on li.district_id = dd.district_id
  and li.funding_year = dd.funding_year

  LEFT JOIN expiration ee
  on ee.district_id = dd.district_id
  and ee.funding_year = dd.funding_year

  WHERE dd.funding_year = 2019
  and dd.in_universe = true
  and dd.district_type = 'Traditional'
),

first_table_18 as (
 SELECT
  dd.district_id,
  dd.funding_year,
  CASE WHEN bb.meeting_2018_goal_oversub = false AND ff.fit_for_ia = true THEN 'not_meeting'
  WHEN ee.year_meeting_1mbps IS NOT NULL THEN 'meeting'
  ELSE 'unknown' END as meeting_2018,
  bb.ia_bw_mbps_total as bw18,
  bb.ia_monthly_cost_total as cost18,
  ff.fit_for_ia,
  ee.year_meeting_1mbps,
  CASE
    WHEN ee.contract_expiring IS NOT NULL THEN ee.contract_expiring
    WHEN li.most_recent_primary_ia_contract_end_date <= '2020-06-30' THEN 'expiring'
    WHEN (li.most_recent_primary_ia_contract_end_date > '2020-06-30' OR li.most_recent_primary_ia_contract_end_date ISNULL) THEN 'not expiring'
    ELSE 'unknown' END as having_contract_expiring,
  li.consortia_applied_ia_lines

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

  LEFT JOIN expiration ee
  on ee.district_id = dd.district_id
  and ee.funding_year = dd.funding_year

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
  count(distinct pr.peer_id) FILTER (WHERE pr.peer_ia_bw_mbps_total >= dbw.projected_bw_fy2018) AS num_peer_deals,
  count(1) FILTER (WHERE pr.peer_service_provider = sp.primary_sp AND pr.peer_ia_bw_mbps_total >= dbw.projected_bw_fy2018) AS same_sp_deal,
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
  dot.c1_barriers_to_upgrade

FROM first_table_19 ff

JOIN ps.districts_bw_cost dbw
ON ff.district_id = dbw.district_id

JOIN ps.districts dd
ON dd.district_id = dbw.district_id
AND dd.funding_year = dbw.funding_year

left JOIN ps.districts_sp_assignments sp
ON dbw.district_id = sp.district_id
AND dbw.funding_year = sp.funding_year

LEFT JOIN ps.districts_peers_ranks pr
ON dbw.district_id = pr.district_id
AND dbw.funding_year = pr.funding_year

LEFT JOIN ps.districts_outreach dot
ON dot.district_id = dd.district_id
AND dot.funding_year = dd.funding_year

-- WHERE ff.fit_for_ia = true
WHERE dbw.funding_year = 2019
GROUP BY 1,2,3,4,7,8,9,10,11,12,13,14,15,16,17
),

second_table_18 as (

SELECT
  ff.district_id,
  ff.meeting_2018,
  ff.year_meeting_1mbps,
  ff.having_contract_expiring,
  count(distinct pr.peer_id) FILTER (WHERE pr.peer_ia_bw_mbps_total >= dbw.projected_bw_fy2018) AS num_peer_deals,
  count(1) FILTER (WHERE pr.peer_service_provider = sp.primary_sp AND pr.peer_ia_bw_mbps_total >= dbw.projected_bw_fy2018) AS same_sp_deal,
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
  dot.c1_barriers_to_upgrade

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

LEFT JOIN ps.districts_peers_ranks pr
ON dbw.district_id = pr.district_id
AND dbw.funding_year = pr.funding_year

LEFT JOIN ps.districts_outreach dot
ON dot.district_id = dd.district_id
AND dot.funding_year = dd.funding_year

WHERE ff.meeting_2018 = 'not_meeting'
AND ff.fit_for_ia = true
AND dbw.funding_year = 2018
GROUP BY 1,2,3,4,7,8,9,10,11,12,13,14,15,16,17
)

select
  'state' as unit,
  s19.state_code as name,
  count(s19.district_id) as num_districts,
  count(s19.district_id)/(select count(*) from first_table_18 f18

      LEFT JOIN second_table_18 s18
      on f18.district_id = s18.district_id

      LEFT JOIN first_table_19 f19
      on f19.district_id = f18.district_id

      LEFT JOIN second_table_19 s19
      on s19.district_id = s18.district_id

      LEFT join ps.districts_bw_cost d
      on d.district_id = f18.district_id
      and d.funding_year = 2019

      WHERE s18.having_contract_expiring = 'expiring'
      AND s18.meeting_2018 = 'not_meeting'
      and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
      AND s18.num_peer_deals > 0
      AND s18.same_sp_deal > 0
      AND f18.fit_for_ia = true
      AND f19.fit_for_ia = true)::decimal as percent

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

WHERE s18.having_contract_expiring = 'expiring'
AND s18.meeting_2018 = 'not_meeting'
and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
AND s18.num_peer_deals > 0
AND s18.same_sp_deal > 0
AND f18.fit_for_ia = true
AND f19.fit_for_ia = true

GROUP BY s19.state_code

UNION ALL

select
  'sp' as unit,
  s19.primary_sp as name,
  count(s19.district_id) as num_districts,
  count(s19.district_id)/(select count(*) from first_table_18 f18

      LEFT JOIN second_table_18 s18
      on f18.district_id = s18.district_id

      LEFT JOIN first_table_19 f19
      on f19.district_id = f18.district_id

      LEFT JOIN second_table_19 s19
      on s19.district_id = s18.district_id

      LEFT join ps.districts_bw_cost d
      on d.district_id = f18.district_id
      and d.funding_year = 2019

      WHERE s18.having_contract_expiring = 'expiring'
      AND s18.meeting_2018 = 'not_meeting'
      and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
      AND s18.num_peer_deals > 0
      AND s18.same_sp_deal > 0
      AND f18.fit_for_ia = true
      AND f19.fit_for_ia = true)::decimal as percent

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

WHERE s18.having_contract_expiring = 'expiring'
AND s18.meeting_2018 = 'not_meeting'
and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
AND s18.num_peer_deals > 0
AND s18.same_sp_deal > 0
AND f18.fit_for_ia = true
AND f19.fit_for_ia = true

GROUP BY s19.primary_sp

UNION ALL

select
  'size' as unit,
  s19.size as name,
  count(s19.district_id) as num_districts,
  count(s19.district_id)/(select count(*) from first_table_18 f18

      LEFT JOIN second_table_18 s18
      on f18.district_id = s18.district_id

      LEFT JOIN first_table_19 f19
      on f19.district_id = f18.district_id

      LEFT JOIN second_table_19 s19
      on s19.district_id = s18.district_id

      LEFT join ps.districts_bw_cost d
      on d.district_id = f18.district_id
      and d.funding_year = 2019

      WHERE s18.having_contract_expiring = 'expiring'
      AND s18.meeting_2018 = 'not_meeting'
      and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
      AND s18.num_peer_deals > 0
      AND s18.same_sp_deal > 0
      AND f18.fit_for_ia = true
      AND f19.fit_for_ia = true)::decimal as percent

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

WHERE s18.having_contract_expiring = 'expiring'
AND s18.meeting_2018 = 'not_meeting'
and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
AND s18.num_peer_deals > 0
AND s18.same_sp_deal > 0
AND f18.fit_for_ia = true
AND f19.fit_for_ia = true

GROUP BY s19.size

UNION ALL

select
  'locale' as unit,
  s19.locale as name,
  count(s19.district_id) as num_districts,
  count(s19.district_id)/(select count(*) from first_table_18 f18

      LEFT JOIN second_table_18 s18
      on f18.district_id = s18.district_id

      LEFT JOIN first_table_19 f19
      on f19.district_id = f18.district_id

      LEFT JOIN second_table_19 s19
      on s19.district_id = s18.district_id

      LEFT join ps.districts_bw_cost d
      on d.district_id = f18.district_id
      and d.funding_year = 2019

      WHERE s18.having_contract_expiring = 'expiring'
      AND s18.meeting_2018 = 'not_meeting'
      and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
      AND s18.num_peer_deals > 0
      AND s18.same_sp_deal > 0
      AND f18.fit_for_ia = true
      AND f19.fit_for_ia = true)::decimal as percent

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

WHERE s18.having_contract_expiring = 'expiring'
AND s18.meeting_2018 = 'not_meeting'
and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
AND s18.num_peer_deals > 0
AND s18.same_sp_deal > 0
AND f18.fit_for_ia = true
AND f19.fit_for_ia = true

GROUP BY s19.locale

UNION ALL

select
  'consortia_applied' as unit,
  (CASE when f19.consortia_applied_ia_lines > 0 THEN TRUE ELSE FALSE END)::varchar as name,
  count(s19.district_id) as num_districts,
  count(s19.district_id)/(select count(*) from first_table_18 f18

      LEFT JOIN second_table_18 s18
      on f18.district_id = s18.district_id

      LEFT JOIN first_table_19 f19
      on f19.district_id = f18.district_id

      LEFT JOIN second_table_19 s19
      on s19.district_id = s18.district_id

      LEFT join ps.districts_bw_cost d
      on d.district_id = f18.district_id
      and d.funding_year = 2019

      WHERE s18.having_contract_expiring = 'expiring'
      AND s18.meeting_2018 = 'not_meeting'
      and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
      AND s18.num_peer_deals > 0
      AND s18.same_sp_deal > 0
      AND f18.fit_for_ia = true
      AND f19.fit_for_ia = true)::decimal as percent

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

WHERE s18.having_contract_expiring = 'expiring'
AND s18.meeting_2018 = 'not_meeting'
and (s19.meeting_2018 = 'meeting' or d.meeting_2018_goal_oversub = true)
AND s18.num_peer_deals > 0
AND s18.same_sp_deal > 0
AND f18.fit_for_ia = true
AND f19.fit_for_ia = true

GROUP BY name

ORDER BY unit, percent DESC
