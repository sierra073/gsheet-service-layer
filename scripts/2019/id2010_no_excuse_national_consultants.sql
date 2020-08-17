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

upgrade_tracker as (
SELECT
dd.district_id,
CASE
  WHEN bw19.ia_bw_mbps_total > bw18.ia_bw_mbps_total
    AND ff19.fit_for_ia = true
    AND ff18.fit_for_ia = true
    then 2019
  WHEN bw18.ia_bw_mbps_total > bw17.ia_bw_mbps_total
    AND ff18.fit_for_ia = true
    AND ff17.fit_for_ia = true
    then 2018
  WHEN bw17.ia_bw_mbps_total > bw16.ia_bw_mbps_total
    AND ff17.fit_for_ia = true
    AND ff16.fit_for_ia = true
    then 2017
  WHEN bw16.ia_bw_mbps_total > bw15.ia_bw_mbps_total
    AND ff16.fit_for_ia = true
    AND ff15.fit_for_ia = true
    then 2016
END as last_upgrade

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
),


expiration as (
SELECT dd.district_id,
dd.funding_year,
yy.year_meeting_1mbps,
CASE
  WHEN yy.year_meeting_1mbps = 2016 AND dl15.most_recent_ia_contract_end_date >= '2016-06-30' THEN 'expiring'
  WHEN yy.year_meeting_1mbps = 2017 AND dl16.most_recent_ia_contract_end_date >= '2017-06-30' THEN 'expiring'
  WHEN yy.year_meeting_1mbps = 2018 AND dl17.most_recent_ia_contract_end_date >= '2018-06-30' THEN 'expiring'
  WHEN yy.year_meeting_1mbps = 2019 AND dl18.most_recent_ia_contract_end_date >= '2019-06-30' THEN 'expiring'
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
  dd.size,
  dd.locale,
  CASE WHEN ee.year_meeting_1mbps IS NOT NULL THEN dee.num_students
  ELSE dd.num_students END as num_students,
  CASE WHEN bb.meeting_2018_goal_oversub = false AND ff.fit_for_ia = true THEN 'not_meeting'
  WHEN (ee.year_meeting_1mbps IS NOT NULL OR bb.meeting_2018_goal_oversub = true) THEN 'meeting'
  ELSE 'unknown' END as meeting_2018,
  bb.ia_bw_mbps_total as bw19,
  bb.ia_monthly_cost_total as cost19,
  ff.fit_for_ia,
  ee.year_meeting_1mbps,
  CASE
    WHEN ee.contract_expiring IS NOT NULL THEN ee.contract_expiring
    WHEN li.most_recent_ia_contract_end_date <= '2020-06-30' THEN 'expiring'
    WHEN (li.most_recent_ia_contract_end_date > '2020-06-30' OR li.most_recent_ia_contract_end_date ISNULL) THEN 'not expiring'
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

  LEFT JOIN expiration ee
  on ee.district_id = dd.district_id
  and ee.funding_year = dd.funding_year

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
  CASE WHEN ee.year_meeting_1mbps IS NOT NULL THEN dee.num_students
  ELSE dd.num_students END as num_students,
  CASE WHEN bb.meeting_2018_goal_oversub = false AND ff.fit_for_ia = true THEN 'not_meeting'
  WHEN ee.year_meeting_1mbps IS NOT NULL THEN 'meeting'
  ELSE 'unknown' END as meeting_2018,
  bb.ia_bw_mbps_total as bw18,
  bb.ia_monthly_cost_total as cost18,
  ff.fit_for_ia,
  ee.year_meeting_1mbps,
  CASE
    WHEN ee.contract_expiring IS NOT NULL THEN ee.contract_expiring
    WHEN li.most_recent_ia_contract_end_date <= '2020-06-30' THEN 'expiring'
    WHEN (li.most_recent_ia_contract_end_date > '2020-06-30' OR li.most_recent_ia_contract_end_date ISNULL) THEN 'not expiring'
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

  LEFT JOIN expiration ee
  on ee.district_id = dd.district_id
  and ee.funding_year = dd.funding_year

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
  count(distinct pd.deal) as num_peer_deals,
  count(distinct pd.deal) filter(where pd.current_provider = true) as same_sp_deal,
  dd.state_code,
  dd.size,
  dd.locale,
  dd.ulocal,
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

LEFT JOIN ps.peer_deal_line_items pd
ON dbw.district_id = pd.district_id
AND dbw.funding_year = pd.funding_year

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
  dd.ulocal,
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
  count(distinct pr.peer_id) FILTER (WHERE pr.peer_ia_bw_mbps_total >= dbw.projected_bw_fy2018) AS num_peer_deals,
  count(1) FILTER (WHERE pr.peer_service_provider = sp.primary_sp AND pr.peer_ia_bw_mbps_total >= dbw.projected_bw_fy2018) AS same_sp_deal,
  max(dbw.ia_monthly_cost_total - pr.peer_ia_monthly_cost_total) filter(where pr.peer_service_provider = sp.primary_sp AND pr.peer_ia_bw_mbps_total >= dbw.projected_bw_fy2018) as best_peer_deal_savings,
  dd.state_code,
  dd.size,
  dd.locale,
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

LEFT JOIN ps.districts_peers_ranks pr
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
  dd.num_campuses,
  dd.consortium_affiliation,
  sp.primary_sp,
  sp.primary_sp_bandwidth,
  sp.service_provider_id,
  dot.c1_barriers_to_upgrade,
  ff.num_students
),

get_extrap AS (
    SELECT
    count(distinct f19.district_id)::numeric/count(distinct f19.district_id) filter(where f19.fit_for_ia = true and f18.fit_for_ia = true)::numeric as dist_multiplier

    from first_table_19 f19

    left join first_table_18 f18
    on f19.district_id = f18.district_id

),

state_network_exc as ( -- added to exclude state network states
    SELECT
    district_id,
    ss.state_network as state_network_tf

    from ps.districts dd

    left join ps.states_static ss
    on dd.state_code = ss.state_code

    where funding_year = 2019
),

consultants as (
SELECT
district_id,
consultant_id,
consultant_name,
funding_year

from ps.consultants_districts

where category_of_service = 1
and funding_year >= 2018
),

all_groups_metrics as (
  select
    s19.state_code,
    f19.district_id,
    f19.size,
    f19.locale,
    s19.ulocal,
    f19.num_students,
    bw18,
    cost18,
    bw19,
    cost19,
    s18.primary_sp as sp_18,
    s18.primary_sp_bandwidth as sp_bw_18,
    s19.primary_sp as sp_19,
    s19.primary_sp_bandwidth as sp_bw_19,
    ct.consultant_name as consultant_19,
    sn.state_network_tf,
    s18.num_peer_deals as num_peer_deals_18,
    s19.num_peer_deals as num_peer_deals_19,
    s19.having_contract_expiring,
    ut.last_upgrade,
    s18.same_sp_deal,
    s18.best_peer_deal_savings,
    s18.best_peer_deal_savings::numeric/cost18::numeric as best_peer_deal_savings_perc,
    CASE
      WHEN (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false)
              AND (f19.bw19 <= f18.bw18)
              AND (s19.num_peer_deals <> 0 AND s19.same_sp_deal <> 0)
              AND s19.having_contract_expiring = 'expiring'
        then 'No Upgrade No Excuse'
      WHEN (s19.meeting_2018 = 'not_meeting' OR d.meeting_2018_goal_oversub is false)
              AND (f19.bw19 > f18.bw18)
        then 'Upgraded But Still Not Meeting'
      WHEN (s19.meeting_2018 = 'meeting' OR d.meeting_2018_goal_oversub is true)
        then 'Upgraded Now Meeting'
      WHEN (s19.same_sp_deal = 0) OR (f19.bw19 <= f18.bw18 AND s19.num_peer_deals <> 0 AND s19.same_sp_deal <> 0 AND s19.having_contract_expiring != 'expiring')
        then 'Some Excuse'
    END as funnel_group

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

    LEFT JOIN state_network_exc sn
    on f18.district_id = sn.district_id

    LEFT JOIN upgrade_tracker ut
    on f18.district_id = ut.district_id

    LEFT JOIN consultants ct
    on f18.district_id = ct.district_id
    and ct.funding_year = 2019

  WHERE s18.having_contract_expiring = 'expiring'
    AND s18.meeting_2018 = 'not_meeting'
    AND s18.num_peer_deals > 0
    AND s18.same_sp_deal > 0
    AND f18.fit_for_ia = true
    AND f19.fit_for_ia = true
    AND sn.state_network_tf = false

group by s19.state_code,
    f19.district_id,
    f19.size,
    f19.locale,
    s19.ulocal,
    f19.num_students,
    bw18,
    cost18,
    bw19,
    cost19,
    s18.primary_sp,
    s18.primary_sp_bandwidth,
    s19.primary_sp,
    s19.primary_sp_bandwidth,
    sn.state_network_tf,
    s18.num_peer_deals,
    s19.num_peer_deals,
    s19.having_contract_expiring,
    ut.last_upgrade,
    s18.same_sp_deal,
    s18.best_peer_deal_savings,
    s19.meeting_2018,
    d.meeting_2018_goal_oversub,
    s19.same_sp_deal,
    s19.having_contract_expiring,
    ct.consultant_name),

consultant_percents as (
    with district_consultant as (
        SELECT
        d.district_id,
        c.consultant_name

        from ps.districts d

        left join (SELECT
                  district_id,
                  consultant_id,
                  consultant_name,
                  funding_year

                  from ps.consultants_districts

                  where category_of_service = 1
                  and funding_year >= 2018) c
        on d.district_id = c.district_id
        and d.funding_year = c.funding_year

        left join ps.districts_fit_for_analysis fit
        on d.district_id = fit.district_id
        and d.funding_year = fit.funding_year

        where d.funding_year = 2019
        and d.in_universe
        and d.district_type = 'Traditional'
        and fit.fit_for_ia
        and d.state_code not in ('IA','AL','CT','DE','GA','KY','ME','MO','MS','NC','ND','NE','RI','SD','SC','TN','UT','WA','WV','WY'))

    SELECT
      CASE
        WHEN consultant_name is NULL
          then 'No Consultant'
        ELSE consultant_name
      END as consultant_name,
      count(distinct district_id) as district_count,
      count(distinct district_id)::numeric/(select count(*)::numeric from district_consultant) as natl_perc

    FROM district_consultant

    group by consultant_name

    order by 3 desc
)

SELECT
  CASE
    WHEN am.consultant_19 is NULL
      then 'No Consultant'
    ELSE am.consultant_19
  END as consultant,
  count(distinct am.district_id)*(select * from get_extrap) as no_excuse_district_count,
  count(distinct am.district_id)::numeric/(select count(distinct district_id)::numeric from all_groups_metrics where funnel_group = 'No Upgrade No Excuse') as "% of No Excuse",
  natl_perc,
  count(distinct am.district_id)::numeric/cp.district_count::numeric as "% of clients having No Excuse",
  array_agg(distinct state_code) as states

  from all_groups_metrics am

  join consultant_percents cp
  on am.consultant_19 = cp.consultant_name
  and cp.consultant_name is not null

  where funnel_group = 'No Upgrade No Excuse'

  group by consultant,natl_perc,cp.district_count

  order by no_excuse_district_count desc
