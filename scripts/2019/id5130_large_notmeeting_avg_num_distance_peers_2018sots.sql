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
    sp.primary_sp as primary_sp,
    pr.peer_id,
    pr.peer_distance,
    pr.rank_distance,
    psp.primary_sp as peer_primary_sp
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
  --to determine the district service provider
  JOIN ps.districts_sp_assignments sp
  ON fit.district_id = sp.district_id
  AND fit.funding_year = sp.funding_year
  --to determine if the district is meeting goals
  left JOIN ps.districts_peers_ranks pr
  ON fit.district_id = pr.district_id
  AND fit.funding_year = pr.funding_year
  --to determine the district service provider
  left JOIN ps.districts_sp_assignments psp
  ON pr.peer_id = psp.district_id
  AND pr.funding_year = psp.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
),

district_num_peers as (
select district_id, 
count(peer_id) as num_peers
from subset
where fit_for_ia = true
and meeting_2014_goal_no_oversub = false 
and num_students > 9000
and peer_id is not null
group by 1)

select 
'avg # peers' as metric,
avg(num_peers) as avg_value,
median(num_peers) as median_value
from district_num_peers
group by 1

union

select
'avg. distance to closest peer' as metric,
avg(peer_distance/1609.34) as avg_value,
median(peer_distance/1609.34) as median_value
from subset
where fit_for_ia = true
and meeting_2014_goal_no_oversub = false 
and num_students > 9000
and peer_id is not null
and rank_distance = 1
group by 1
