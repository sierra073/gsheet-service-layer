with subset as (
  select 
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bc.meeting_2018_goal_oversub,
    fit.fit_for_ia,
    dd.num_students,
    dd.size
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
    round(sample_groups.num_students_not_meeting_100*sample_pop.population_students::numeric/sample_pop.sample_students,-5) as num_students_not_meeting_100,
    round(sample_groups.num_students_meeting_1m*sample_pop.population_students::numeric/sample_pop.sample_students,-5) as num_students_meeting_1m,
    round(sample_groups.num_districts_not_meeting_100*sample_pop.population_districts::numeric/sample_pop.sample_districts,0) as num_districts_not_meeting_100,
    round(sample_groups.num_districts_not_meeting_100_gt_9k_st*sample_pop.population_districts::numeric/sample_pop.sample_districts,0) as num_districts_not_meeting_100_gt_9k_st
    from (
      select 
        sum(num_students) FILTER (WHERE meeting_2014_goal_no_oversub = false) as num_students_not_meeting_100,
        sum(num_students) FILTER (WHERE meeting_2018_goal_oversub = true) as num_students_meeting_1m,
        count(*) FILTER (WHERE meeting_2014_goal_no_oversub = false) as num_districts_not_meeting_100,
        count(*) FILTER ( WHERE meeting_2014_goal_no_oversub = false 
                          and num_students > 9000) as num_districts_not_meeting_100_gt_9k_st
      from subset
      where fit_for_ia = true
    ) sample_groups
    join (
      select 
        sum(num_students) as population_students,
        sum(num_students) FILTER (WHERE fit_for_ia = true) as sample_students,
        count(*) as population_districts,
        count(*) FILTER (WHERE fit_for_ia = true) as sample_districts
      from subset
    ) sample_pop  
    on true
