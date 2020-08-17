with subset as (
  select 
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bcpy.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_py, 
    fit.fit_for_ia,
    fitpy.fit_for_ia as fit_for_ia_py,
    dd.num_students,
    d470.num_broadband_470s,
    d470.num_fiber_470s,    
    d470ok.low_range_470s,
    d470ok.max_meeting_goals_470s,
    d470ok.min_meeting_goals_470s,
    d470ok.internet_function_470s,
    dc.district_id is not null as consultant_used,
    dd.technology_contact,
    fib.hierarchy_ia_connect_category,
    fibpy.hierarchy_ia_connect_category as hierarchy_ia_connect_category_py
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
  --to filter for Traditional districts in universe, and student quantification
  LEFT JOIN ps.districts ddpy
  ON fit.district_id = ddpy.district_id
  AND fit.funding_year - 1 = ddpy.funding_year
  --to determine if the district is meeting goals
  LEFT JOIN ps.districts_bw_cost bcpy
  ON fit.district_id = bcpy.district_id
  AND fit.funding_year - 1 = bcpy.funding_year
  --to determine if the district was eligible for upgrade last year
  LEFT JOIN ps.districts_fit_for_analysis fitpy
  ON fit.district_id = fitpy.district_id
  AND fit.funding_year - 1 = fitpy.funding_year
  --to determine if the district changed technology
  LEFT JOIN ps.districts_fiber fib
  ON fit.district_id = fib.district_id
  AND fit.funding_year = fib.funding_year
  --to determine if the district changed technology
  LEFT JOIN ps.districts_fiber fibpy
  ON fit.district_id = fibpy.district_id
  AND fit.funding_year - 1 = fibpy.funding_year
  --to determine if broadband or fiber 470 was submitted
  LEFT JOIN ps.districts_470s d470
  ON fit.district_id = d470.district_id
  AND fit.funding_year = d470.funding_year
  --to determine type of 470 submitted
  LEFT JOIN (
    select 
      dli.district_id,
      dli.funding_year,
      count(*) filter (where dli.min_capacity_mbps > 0 
                      and dli.max_capacity_mbps/dli.min_capacity_mbps < 1000) as low_range_470s,
      count(*) filter (where dli.max_capacity_mbps >= d.num_students*.1) as max_meeting_goals_470s,
      count(*) filter (where dli.min_capacity_mbps >= d.num_students*.1) as min_meeting_goals_470s,
      count(*) filter (where lower(li.function) ilike '%internet%'
                      OR  lower(li.function) ilike '%fiber%'
                      OR  lower(li.function) ilike '%self-provision%'
                      OR  lower(li.function) ilike '%transport only%') as internet_function_470s
    from dwh.ft_districts_470_line_items dli
    left join ps.districts d
    on dli.funding_year = d.funding_year
    and dli.district_id = d.district_id
    LEFT JOIN dwh.dt_470_line_items li
    ON dli.funding_year = li.funding_year
    AND dli.form_470_line_item_id = li.form_470_line_item_id
    group by 1,2
  ) d470ok
  ON fit.district_id = d470ok.district_id
  and fit.funding_year = d470ok.funding_year
  --to determine if consultant used
  LEFT JOIN (
    select distinct
      district_id,
      funding_year
    from ps.consultants_districts
  ) dc
  ON fit.district_id = dc.district_id
  and fit.funding_year = dc.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
),

newly_meeting as (
  select 
    round(sample_groups.num_districts*sample_pop.population_districts::numeric/sample_pop.sample_districts,0)- 
       meeting_2018_pop.districts_meeting_100kbps 
        as num_districts
    from (
      select 
        meeting_2014_goal_no_oversub,
        count(*) as num_districts
      from subset
      where fit_for_ia = true
      group by 1
    ) sample_groups
    join (
      select 
        count(*) as population_districts,
        count(*) FILTER (WHERE fit_for_ia = true) as sample_districts
      from subset
    ) sample_pop  
    on true
    left join (
      select districts_meeting_100kbps
      from ps.state_snapshot_frozen_sots
      where funding_year = 2018
      and state_code = 'ALL'
    ) meeting_2018_pop
    on true
    where sample_groups.meeting_2014_goal_no_oversub = true
)

select
  meeting_2014_goal_no_oversub,
  sum(1::numeric) filter (where technology_contact = true)/sum(1::numeric) 
    as pct_districts_technology_contact,
  sum(1::numeric) filter (where consultant_used = true)/sum(1::numeric) 
    as pct_districts_consultant_used,
  sum(1::numeric) filter (where num_broadband_470s > 0)/sum(1::numeric) 
    as pct_districts_bb_470,
  sum(1::numeric) filter (where num_broadband_470s > 0)/sum(1::numeric) * newly_meeting.num_districts
    as num_districts_bb_470,
  sum(1::numeric) filter (where num_fiber_470s > 0)/sum(1::numeric) filter (where num_broadband_470s > 0) 
    as pct_districts_fiber_470_of_bb,
  sum(1::numeric) filter (where low_range_470s > 0)/sum(1::numeric) filter (where num_broadband_470s > 0)
    as pct_districts_low_range_470_of_bb,
  sum(1::numeric) filter (where max_meeting_goals_470s > 0)/sum(1::numeric) filter (where num_broadband_470s > 0) 
    as pct_districts_max_meeting_goals_470_of_bb,
  sum(1::numeric) filter (where min_meeting_goals_470s > 0)/sum(1::numeric) filter (where num_broadband_470s > 0)
    as pct_districts_min_meeting_goals_470_of_bb,
  sum(1::numeric) filter (where min_meeting_goals_470s > 0)/sum(1::numeric) filter (where num_broadband_470s > 0) * newly_meeting.num_districts
    as num_districts_min_meeting_goals_470_of_bb,
  sum(1::numeric) filter (where consultant_used = true
                          and min_meeting_goals_470s > 0)/sum(1::numeric) filter (where min_meeting_goals_470s > 0)
    as pct_districts_used_consultant_of_min_meeting_goals_470,
  sum(1::numeric) filter (where internet_function_470s > 0)/sum(1::numeric) filter (where num_broadband_470s > 0)
    as pct_districts_internet_function_470_of_bb,
  sum(1::numeric) filter (where hierarchy_ia_connect_category_py != hierarchy_ia_connect_category)/sum(1::numeric)
    as pct_districts_changed_technology,
  sum(1::numeric) filter (where hierarchy_ia_connect_category_py != hierarchy_ia_connect_category)/sum(1::numeric)* newly_meeting.num_districts
    as num_districts_changed_technology
from subset
left join newly_meeting
on subset.meeting_2014_goal_no_oversub = true
where fit_for_ia = true
and fit_for_ia_py = true
and meeting_2014_goal_no_oversub_py = false
group by 1, newly_meeting.num_districts