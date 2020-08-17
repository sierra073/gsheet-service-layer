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
),

overall as (
  select 
    round(sample_groups.num_districts*sample_pop.population_districts::numeric/sample_pop.sample_districts,0) as num_districts
  from (
    select 
      meeting_2014_goal_no_oversub,
      count(distinct district_id) as num_districts
    from subset
    where fit_for_ia = true
    group by 1
  ) sample_groups
  join (
    select 
      count(distinct district_id) as population_districts,
      count(distinct district_id) FILTER (WHERE fit_for_ia = true) as sample_districts
    from subset
  ) sample_pop  
  on true
  where sample_groups.meeting_2014_goal_no_oversub = false
),

largest_split as (
  select  
    subset.peer_id is null as no_peer,
    count(distinct subset.district_id) as num_districts,
    count(distinct subset.district_id) / tot.sample as pct_districts
  from subset
  join (
    select count(distinct district_id)::numeric as sample
    from subset
    where fit_for_ia = true
    and meeting_2014_goal_no_oversub = false 
    and num_students > 9000
  ) tot
  on true
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false 
  and num_students > 9000
  group by 1, sample
),

overall_split as (
  select  
    subset.peer_id is null as no_peer,
    count(distinct subset.district_id) / tot.sample * overall.num_districts as num_districts,
    count(distinct subset.district_id) / tot.sample as pct_districts
  from subset
  join (
    select count(distinct district_id)::numeric as sample
    from subset
    where fit_for_ia = true
    and meeting_2014_goal_no_oversub = false 
  ) tot
  on true
  join overall
  on true
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false 
  group by 1, tot.sample, overall.num_districts
)

select
  os.no_peer,
  round(os.num_districts - ls.num_districts,0) as num_districts,
  round(os.num_districts - ls.num_districts,0) / 
    (overall.num_districts - largest.num_districts) as pct_districts

from overall_split os
join largest_split ls
on os.no_peer = ls.no_peer
join overall
on true
join (
  select sum(num_districts) as num_districts
  from largest_split
) largest
on true