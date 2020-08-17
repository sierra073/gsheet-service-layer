  with unnest_cck12_peers as (
  select distinct on (p.district_id, p.funding_year)
    p.district_id,
    p.funding_year,
    p.peer_id,
    bw.ia_annual_cost_total as incr_cost_peer_ia_annual_cost_total
  from (
    select
      district_id,
      funding_year,
      unnest(bandwidth_suggested_districts) as peer_id
    from ps.districts_peers
  ) p
  join ps.districts_bw_cost bw
  on p.peer_id = bw.district_id
  and p.funding_year = bw.funding_year
  join ps.districts d
  on p.peer_id = d.district_id
  and p.funding_year = d.funding_year
  join ps.districts_fit_for_analysis fit
  on p.peer_id = fit.district_id
  and p.funding_year = fit.funding_year
  where fit.fit_for_ia = true
  and fit.fit_for_ia_cost = true
  and d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
  order by p.district_id, p.funding_year, bw.ia_annual_cost_total desc
),

subset as (
  select
    fit.district_id,
    dd.consortium_affiliation_ids,
    bc.meeting_2014_goal_no_oversub,
    fit.fit_for_ia,
    dd.num_students,
    pr.district_id is not null as has_no_cost_peer_deal,
    cp.incr_cost_peer_ia_annual_cost_total,
    case
      when fit.fit_for_ia_cost = true
      and bc.ia_annual_cost_erate != 0
        then (cp.incr_cost_peer_ia_annual_cost_total*(bc.ia_annual_cost_erate-bc.ia_funding_requested_erate)/bc.ia_annual_cost_erate) -
              case
                when bc.ia_funding_requested_erate > bc.ia_annual_cost_total
                  then 0
                else (bc.ia_annual_cost_total-bc.ia_funding_requested_erate)
              end
      when fit.fit_for_ia_cost = true
        then cp.incr_cost_peer_ia_annual_cost_total*(1-dd.c1_discount_rate)
    end as peer_oop_increase
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
  --to determine how much more those without peer deals need to spend
  LEFT JOIN unnest_cck12_peers cp
  ON fit.district_id = cp.district_id
  AND fit.funding_year = cp.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
),

not_meeting_not_lrg_only as (
  select
    unnest(string_to_array(consortium_affiliation_ids, ' | ')) as consortium_id,
    district_id,
    'consortia' as subgroup,
    incr_cost_peer_ia_annual_cost_total,
    peer_oop_increase,
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
        then 'no consortia, peer deal'
      else 'increase budget'
    end as subgroup,
    incr_cost_peer_ia_annual_cost_total,
    peer_oop_increase,
    num_students
  from subset
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false
  and num_students <= 9000
  and consortium_affiliation_ids is null
),

assumed_oop as (
  select
    sum(peer_oop_increase)/sum(incr_cost_peer_ia_annual_cost_total) as pct_incr_of_cost
  from not_meeting_not_lrg_only
  where peer_oop_increase is not null
  and subgroup = 'increase budget'
),

sample as (
  select
    sum(1::numeric) as district_sample,
    sum(case
          when subgroup = 'increase budget'
          and peer_oop_increase is null
            then incr_cost_peer_ia_annual_cost_total*pct_incr_of_cost
          when subgroup = 'increase budget'
            then peer_oop_increase
        end) as increase_budget_peer_oop_increase,
    sum(num_students) FILTER (where subgroup = 'increase budget') as increase_budget_student_sample
  from (
    select distinct district_id, num_students, subgroup, pct_incr_of_cost, peer_oop_increase, incr_cost_peer_ia_annual_cost_total
    from not_meeting_not_lrg_only
    join assumed_oop
    on true
  ) dists
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


select
  not_meeting_not_lrg_only.subgroup,
  count(distinct not_meeting_not_lrg_only.district_id)/sample.district_sample as percent_districts,
  count(distinct not_meeting_not_lrg_only.district_id) as sample_districts,
  count(distinct  case
                    when consortia.pct_districts_meeting_100kbps + consortia.pct_districts_representing_1 = 1
                      then not_meeting_not_lrg_only.district_id
                  end)/sample.district_sample as percent_districts_consortia_all_100kbps,
  count(distinct case
                    when consortia.pct_districts_meeting_100kbps + consortia.pct_districts_representing_1 = 1
                      then not_meeting_not_lrg_only.district_id
                  end) as sample_districts_consortia_all_100kbps,
  count(distinct case
                    when consortia.pct_districts_meeting_100kbps >= .1
                      then not_meeting_not_lrg_only.district_id
                  end)/sample.district_sample as percent_districts_consortia_model_100kbps,
  count(distinct case
                    when consortia.pct_districts_meeting_100kbps >= .1
                      then not_meeting_not_lrg_only.district_id
                  end) as sample_districts_consortia_model_100kbps,
  case
    when not_meeting_not_lrg_only.subgroup = 'increase budget'
            --increase budget cost/student
      then (sample.increase_budget_peer_oop_increase::numeric/sample.increase_budget_student_sample)*
            --assumed pct students not meeting in this group
            not_meeting.num_students*(sample.increase_budget_student_sample::numeric/clean.num_students)
    else 0
  end as extrapolated_oop_increase,
  case
    when not_meeting_not_lrg_only.subgroup = 'increase budget'
      then sample.increase_budget_peer_oop_increase
    else 0
  end sample_oop_increase
from not_meeting_not_lrg_only
left join consortia
on not_meeting_not_lrg_only.consortium_id = consortia.consortium_id
join sample
on true
join assumed_oop
on true
join not_meeting
on true
join (
  select sum(num_students) as num_students
  from subset
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false
) clean
on true
group by
  not_meeting_not_lrg_only.subgroup,
  sample.increase_budget_peer_oop_increase,
  sample.district_sample,
  sample.increase_budget_student_sample,
  not_meeting.num_students,
  clean.num_students