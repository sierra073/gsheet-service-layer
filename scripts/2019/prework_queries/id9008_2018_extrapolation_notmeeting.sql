with districts as (
select
  d.district_id,
  d.num_students,
  dbc.meeting_2018_goal_oversub,
  case when du.path_to_meet_2018_goal_group = 'No Cost Peer Deal' or du.path_to_meet_2018_goal_group = 'New Knapsack Pricing'
    then 'Peer Deal/Affordable'
  else du.path_to_meet_2018_goal_group end as path_to_meet_2018_goal_group,
  fit.fit_for_ia,
  fit.fit_for_ia_cost

  from ps.districts d
  join ps.districts_fit_for_analysis fit
  on d.district_id = fit.district_id
  and d.funding_year = fit.funding_year
  join ps.districts_bw_cost dbc
  on d.district_id = dbc.district_id
  and d.funding_year = dbc.funding_year
  join ps.districts_upgrades du
  on d.district_id = du.district_id
  and d.funding_year = du.funding_year

  where d.funding_year = 2018
  and d.in_universe = true
  and d.district_type = 'Traditional'),

not_meeting_overall as (
  select
    round(sample_groups.num_districts*sample_pop.population_districts::numeric/sample_pop.sample_districts,0) as num_districts
  from (
    select
      meeting_2018_goal_oversub,
      count(distinct district_id) as num_districts
    from districts
    where fit_for_ia = true
    group by 1
  ) sample_groups
  join (
    select
      count(distinct district_id) as population_districts,
      count(distinct district_id) FILTER (WHERE fit_for_ia = true) as sample_districts
    from districts
  ) sample_pop
  on true
  where sample_groups.meeting_2018_goal_oversub = false
),

not_meeting_overall_path as (
  select
    path_to_meet_2018_goal_group,
    count(distinct districts.district_id) / tot.sample * not_meeting_overall.num_districts as num_districts,
    count(distinct districts.district_id) / tot.sample as pct_districts
  from districts
  join (
    select count(distinct district_id)::numeric as sample
    from districts
    where fit_for_ia = true
    and meeting_2018_goal_oversub = false
    and path_to_meet_2018_goal_group is not null
  ) tot
  on true
  join not_meeting_overall
  on true
  where fit_for_ia = true
  and meeting_2018_goal_oversub = false
  and path_to_meet_2018_goal_group is not null
  group by 1, tot.sample, not_meeting_overall.num_districts
)

select * from not_meeting_overall_path
