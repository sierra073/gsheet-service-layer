with subset as (
  select 
    fit.district_id,
    dd.name,
    dd.state_code,
    dd.locale,
    bc.meeting_2014_goal_no_oversub,
    bc.meeting_2018_goal_oversub,
    bc.ia_bandwidth_per_student_kbps,
    bc.ia_monthly_cost_total,
    fit.fit_for_ia,
    dd.num_students,
    dd.size,
    pr.peer_id,
    pdd.name as peer_name,
    pdd.state_code as peer_state_code,
    pdd.locale as peer_locale,
    pdd.num_students as peer_num_students,
    pdd.size as peer_size,
    pbc.ia_bw_mbps_total as peer_ia_bw_mbps_total,
    pbc.ia_monthly_cost_total as peer_ia_monthly_cost_total,
    dd.outreach_status,
    dd.engagement_status,
    dd.account_owner
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
  --to determine if the district is meeting goals
  LEFT JOIN ps.districts_peers_ranks pr
  ON fit.district_id = pr.district_id
  AND fit.funding_year = pr.funding_year
  AND pr.rank_distance = 1
  --to filter for clean districts
  LEFT JOIN ps.districts pdd
  ON pr.peer_id = pdd.district_id
  AND pr.funding_year = pdd.funding_year
  --to determine if the district is meeting goals
  LEFT JOIN ps.districts_bw_cost pbc
  ON pr.peer_id = pbc.district_id
  AND pr.funding_year = pbc.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
)

      select  
        peer_id is null as no_peer,
        count(*) as num_districts,
        count(*) / sample_districts as pct_districts,
        sum(num_students) as num_students,
        sum(num_students) / sample_students as pct_students
      from subset
      join (
        select 
          count(*)::numeric as sample_districts,
          sum(num_students::numeric) as sample_students
        from subset
        where fit_for_ia = true
        and meeting_2014_goal_no_oversub = false 
        and num_students > 9000
      ) tot
      on true
      where fit_for_ia = true
      and meeting_2014_goal_no_oversub = false 
      and num_students > 9000
      group by 1, sample_districts, sample_students