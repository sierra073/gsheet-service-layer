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
    fit.fit_for_ia_cost,
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


final_subset as (
select district_id, 
fit_for_ia,fit_for_ia_cost,
num_students,
meeting_2014_goal_no_oversub,
sum(case when (peer_id is not null) 
and primary_sp = peer_primary_sp then 1 else 0 end) > 0 as has_same_sp_peer_deal
from subset
group by 1,2,3,4,5),

----limited all tables below to the fit for cost sample
overall as (
  select 
    round(sample_groups.num_districts*sample_pop.population_districts::numeric/sample_pop.sample_districts,0) as num_districts
  from (
    select 
      meeting_2014_goal_no_oversub,
      count(distinct district_id) as num_districts
    from final_subset
    where fit_for_ia = true
    and fit_for_ia_cost = true
    group by 1
  ) sample_groups
  join (
    select 
      count(distinct district_id) as population_districts,
      count(distinct district_id) FILTER (WHERE fit_for_ia = true and fit_for_ia_cost = true) as sample_districts
    from final_subset
  ) sample_pop  
  on true
  where sample_groups.meeting_2014_goal_no_oversub = false
),

largest_split as (
  select  has_same_sp_peer_deal,
    count(distinct final_subset.district_id) as num_districts,
    count(distinct final_subset.district_id) / tot.sample as pct_districts
  from final_subset
  join (
    select count(distinct district_id)::numeric as sample
    from final_subset
    where fit_for_ia = true
    and fit_for_ia_cost = true
    and meeting_2014_goal_no_oversub = false 
    and num_students > 9000
  ) tot
  on true
  where fit_for_ia = true
  and fit_for_ia_cost = true
  and meeting_2014_goal_no_oversub = false 
  and num_students > 9000
  group by 1, sample
),

overall_split as (
  select  
    has_same_sp_peer_deal,
    count(distinct final_subset.district_id) / tot.sample * overall.num_districts as num_districts,
    count(distinct final_subset.district_id) / tot.sample as pct_districts
  from final_subset
  join (
    select count(distinct district_id)::numeric as sample
    from final_subset
    where fit_for_ia = true
    and fit_for_ia_cost = true
    and meeting_2014_goal_no_oversub = false 
  ) tot
  on true
  join overall
  on true
  where fit_for_ia = true
  and fit_for_ia_cost = true
  and meeting_2014_goal_no_oversub = false 
  group by 1, tot.sample, overall.num_districts
)

select
  os.has_same_sp_peer_deal,
  round(os.num_districts - ls.num_districts,0) as num_districts,
  round(os.num_districts - ls.num_districts,0) / 
    (overall.num_districts - largest.num_districts) as pct_districts

from overall_split os
join largest_split ls
on os.has_same_sp_peer_deal = ls.has_same_sp_peer_deal
join overall
on true
join (
  select sum(num_districts) as num_districts
  from largest_split
) largest
on true