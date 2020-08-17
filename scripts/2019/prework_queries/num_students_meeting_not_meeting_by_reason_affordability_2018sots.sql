with subset as (
  select 
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bc.ia_bandwidth_per_student_kbps,
    bc.ia_bw_mbps_total,
    bc.ia_monthly_cost_per_mbps,
    bc.ia_monthly_cost_total,
    fit.fit_for_ia,
    fit.fit_for_ia_cost,
    dd.num_students,
    p.bandwidth_suggested_districts
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
  --to determine if the district has a peer deal for bandwidth
  LEFT JOIN ps.districts_peers p
  ON fit.district_id = p.district_id
  AND fit.funding_year = p.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
),

meeting_not_meeting as (
  select 
    case
      when sample_groups.meeting_2014_goal_no_oversub = false
        then 'not meeting goals' 
      else 'meeting goals'
    end as subgroup,
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,-3) as num_students,
    round(sample_groups.num_districts*sample_pop.population_districts::numeric/sample_pop.sample_districts,0) as num_districts,
    round(sample_groups.median_ia_bandwidth_per_student_kbps,0) as median_ia_bandwidth_per_student_kbps,
    round(sample_groups.median_ia_bw_mbps_total,0) as median_ia_bw_mbps_total,
    round(sample_groups.median_ia_monthly_cost_per_mbps,2) as median_ia_monthly_cost_per_mbps,
    round(sample_groups.median_ia_monthly_cost_total,2) as median_ia_monthly_cost_total,
    round(sample_groups.median_ia_monthly_cost_per_student,2) as median_ia_monthly_cost_per_student,
    round(sample_groups.agg_ia_monthly_cost_per_student,2) as agg_ia_monthly_cost_per_student
    from (
      select 
        meeting_2014_goal_no_oversub,
        sum(num_students) as num_students,
        count(*) as num_districts,
        median(ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps,
        median(ia_bw_mbps_total) as median_ia_bw_mbps_total,
        median(ia_monthly_cost_per_mbps) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_per_mbps,
        median(ia_monthly_cost_total) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_total,
        median(ia_monthly_cost_total::numeric/num_students) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_per_student,
        sum(ia_monthly_cost_total)::numeric/sum(num_students) FILTER (WHERE fit_for_ia_cost = true) as agg_ia_monthly_cost_per_student
      from subset
      where fit_for_ia = true
      group by 1
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
),

sample_groups as (
    select 
      case
        when ia_monthly_cost_total::numeric/num_students < .3
          then 'increase budget'
        when bandwidth_suggested_districts is not null
          then 'peer deal' 
        else 'no peer deal'
      end as subgroup,
      sum(num_students) as num_students,
      count(*) as num_districts,
      median(ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps,
      median(ia_bw_mbps_total) as median_ia_bw_mbps_total,
      median(ia_monthly_cost_per_mbps) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_per_mbps,
      median(ia_monthly_cost_total) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_total,
      median(ia_monthly_cost_total::numeric/num_students) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_per_student,
      sum(ia_monthly_cost_total)::numeric/sum(num_students) FILTER (WHERE fit_for_ia_cost = true) as agg_ia_monthly_cost_per_student
    from subset
    where fit_for_ia = true
    and fit_for_ia_cost = true
    and meeting_2014_goal_no_oversub = false
    group by 1
  )

select 
  sample_groups.subgroup,
  round(sample_groups.num_students*meeting_not_meeting.num_students::numeric/samples.num_students,-3) as num_students,
  round(sample_groups.num_districts*meeting_not_meeting.num_districts::numeric/samples.num_districts,0) as num_districts,
  round(sample_groups.median_ia_bandwidth_per_student_kbps,0) as median_ia_bandwidth_per_student_kbps,
  round(sample_groups.median_ia_bw_mbps_total,0) as median_ia_bw_mbps_total,
  round(sample_groups.median_ia_monthly_cost_per_mbps,2) as median_ia_monthly_cost_per_mbps,
  round(sample_groups.median_ia_monthly_cost_total,2) as median_ia_monthly_cost_total,
  round(sample_groups.median_ia_monthly_cost_per_student,2) as median_ia_monthly_cost_per_student,
  round(sample_groups.agg_ia_monthly_cost_per_student,2) as agg_ia_monthly_cost_per_student
  from sample_groups
  join meeting_not_meeting
  on meeting_not_meeting.subgroup = 'not meeting goals'
  join (
  select
    sum(num_students) as num_students,
    sum(num_districts) as num_districts
    from sample_groups
  ) samples
  on true

    UNION

  select *
  from meeting_not_meeting