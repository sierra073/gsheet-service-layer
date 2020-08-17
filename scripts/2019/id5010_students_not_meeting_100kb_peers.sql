with subset as (
  select
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bc.ia_bandwidth_per_student_kbps,
    bc.ia_bw_mbps_total,
    bc.ia_monthly_cost_per_mbps,
    bc.ia_monthly_cost_total,
    fit.fit_for_ia,
    fit.fit_for_ia_cost,
    dd.num_students,
    count(1) FILTER (WHERE p.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS num_peer_deals,
    count(1) FILTER (WHERE p.current_provider_deal = true AND p.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS same_sp_deal,
    df.fiber_target_status,
    df.hierarchy_ia_connect_category
  --to filter for clean districts
  FROM ps.districts_fit_for_analysis fit
  --to filter for Traditional districts in universe, and student quantification
  JOIN ps.districts dd
  ON fit.district_id = dd.district_id
  AND fit.funding_year = dd.funding_year
  --to determine if the district is meeting goals
  JOIN ps.districts_bw_cost bc
  ON fit.district_id = bc.district_id
  AND fit.funding_year = bc.funding_year

  JOIN ps.districts_fiber df
  on df.district_id = fit.district_id
  and df.funding_year = fit.funding_year
  --to determine if the district has a peer deal for bandwidth
  LEFT JOIN ps.districts_upgrades p
  ON fit.district_id = p.district_id
  AND fit.funding_year = p.funding_year

  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true

  group by fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bc.ia_bandwidth_per_student_kbps,
    bc.ia_bw_mbps_total,
    bc.ia_monthly_cost_per_mbps,
    bc.ia_monthly_cost_total,
    fit.fit_for_ia,
    fit.fit_for_ia_cost,
    dd.num_students,
    df.fiber_target_status,
    df.hierarchy_ia_connect_category
),

get_extrap as ( --getting ratio for extrapolation from clean population
          SELECT
          count(district_id)::numeric/(count(district_id) filter(where fit_for_ia = true))::numeric as dist_multiplier,
          sum(num_students)::numeric/(sum(num_students) filter(where fit_for_ia = true))::numeric as stud_multiplier

          from subset
      )

select
  round(count(district_id)*(select dist_multiplier from get_extrap),0) as districts_not_meeting_2014,
  round(sum(num_students)*(select stud_multiplier from get_extrap),0) as students_not_meeting_2014,
  round((count(district_id) filter (where num_peer_deals > 0 and fit_for_ia_cost=True))*(select dist_multiplier from get_extrap), 0) as districts_not_meeting_with_peers,
  round((sum(num_students) filter (where num_peer_deals > 0 and fit_for_ia_cost=True))*(select stud_multiplier from get_extrap), 0) as students_not_meeting_with_peers,
  round((count(district_id) filter (where num_peer_deals = 0 and fit_for_ia_cost=True))*(select dist_multiplier from get_extrap), 0) as districts_not_meeting_no_peers,
  round((sum(num_students) filter (where num_peer_deals = 0 and fit_for_ia_cost=True))*(select stud_multiplier from get_extrap), 0) as students_not_meeting_no_peers,
  round((count(district_id) filter (where hierarchy_ia_connect_category != 'Fiber'))*(select dist_multiplier from get_extrap), 0) as districts_not_meeting_nonfiber_ia,
  round((sum(num_students) filter (where hierarchy_ia_connect_category != 'Fiber'))*(select stud_multiplier from get_extrap), 0) as students_not_meeting_nonfiber_ia

from subset
where meeting_2014_goal_no_oversub = false
and fit_for_ia = true
