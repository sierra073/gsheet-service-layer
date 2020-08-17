with subset as (
  select 
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    fit.fit_for_ia,
    dd.num_students
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
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
)

  select 
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,-5) - ss.students_meeting_100kbps as num_students
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
    join dwh.state_snapshot_frozen_sots ss
    on ss.funding_year = 2013
    and ss.state_code = 'ALL'
    where sample_groups.meeting_2014_goal_no_oversub = true