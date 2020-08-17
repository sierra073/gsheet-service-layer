with subset as (
  select
    fit.district_id,
    dd.consortium_affiliation_ids,
    bc.meeting_2014_goal_no_oversub,
    fit.fit_for_ia,
    dd.num_students,
    pr.district_id is not null as has_no_cost_peer_deal
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
  --to determine if the district has a peer deal
  left join (
    select distinct
      funding_year,
      district_id
    from ps.districts_peers_ranks
  ) pr
  ON fit.district_id = pr.district_id
  AND fit.funding_year = pr.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
),

not_meeting_not_lrg_only as (
  select
    unnest(string_to_array(consortium_affiliation_ids, ' | ')) as consortium_id,
    district_id,
    case
      when  has_no_cost_peer_deal = true
        then 'peer deal'
      else 'increase budget'
    end as subgroup,
    num_students
  from subset
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false
  and num_students <= 9000
  and consortium_affiliation_ids is not null
    UNION
  select
    consortium_affiliation_ids as consortium_id,
    district_id,
    case
      when  has_no_cost_peer_deal = true
        then 'peer deal'
      else 'increase budget'
    end as subgroup,
    num_students
  from subset
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false
  and num_students <= 9000
  and consortium_affiliation_ids is null
),

sample as (
  select
    sum(1::numeric) as district_sample,
    sum(num_students) FILTER (where subgroup = 'increase budget') as increase_budget_student_sample
  from not_meeting_not_lrg_only
),

consortia as (
  select
    ids.consortium_id,
    sum(1::numeric) FILTER ( where meeting_2014_goal_no_oversub = true)/
      sum(1::numeric) as pct_districts_meeting_100kbps,
    1/sum(1::numeric) as pct_districts_representing_1
  from (
    select distinct consortium_id
    from not_meeting_not_lrg_only
  ) ids
  join (
    select
      unnest(string_to_array(consortium_affiliation_ids, ' | ')) as consortium_id,
      district_id,
      meeting_2014_goal_no_oversub
    from subset
    where fit_for_ia = true
  ) metrics
  on ids.consortium_id = metrics.consortium_id
  group by 1
),

not_meeting as (
  select
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,0) as num_students
    from (
      select
        meeting_2014_goal_no_oversub,
        sum(num_students) as num_students
      from subset
      where fit_for_ia = true
      group by 1
    ) sample_groups
    join (
      select
        sum(num_students) as population_students,
        sum(num_students) FILTER (WHERE fit_for_ia = true) as sample_students
      from subset
    ) sample_pop
    on true
    where sample_groups.meeting_2014_goal_no_oversub = false
)


select distinct
  not_meeting_not_lrg_only.*,
  case
    when consortia.pct_districts_meeting_100kbps >= .1
      then true
    else false
  end as model_consortia
from not_meeting_not_lrg_only
left join consortia
on not_meeting_not_lrg_only.consortium_id = consortia.consortium_id
