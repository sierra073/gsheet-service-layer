with subset as (
  select
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bc.ia_bandwidth_per_student_kbps,
    bc.ia_bw_mbps_total,
    bc.ia_monthly_cost_per_mbps,
    bc.ia_monthly_cost_total,
    bcpy.ia_monthly_cost_total as ia_monthly_cost_total_py,
    fit.fit_for_ia,
    fit.fit_for_ia_cost,
    fitpy.fit_for_ia as fit_for_ia_py,
    fitpy.fit_for_ia_cost as fit_for_ia_cost_py,
    d470.num_broadband_470s,
    up.upgrade_indicator,
    dl.ia_frns_received_zero_bids,
    dd.num_students,
    --determined after discussing with jason and brian
    dl.fiber_internet_upstream_lines * 100000 +
    dl.fixed_wireless_internet_upstream_lines * 1000 +
    dl.cable_internet_upstream_lines * 1000 +
    dl.copper_internet_upstream_lines * 100 +
    dl.satellite_lte_internet_upstream_lines * 40 as bandwidth_maximum,
    (dd.num_students * .1) as bandwidth_needed_for_2014,
    sp.primary_sp,
    d470ay.district_id is null as no_470_indicator
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
  --to determine if the district submitted a form 470 for broadband
  JOIN ps.districts_470s d470
  ON fit.district_id = d470.district_id
  AND fit.funding_year = d470.district_funding_year
  --to determine if the district submitted a form 471 for internet and received 0 bids
  JOIN ps.districts_lines dl
  ON fit.district_id = dl.district_id
  AND fit.funding_year = dl.funding_year
  --to determine if the district upgraded bw
  JOIN ps.districts_upgrades up
  ON fit.district_id = up.district_id
  AND fit.funding_year = up.funding_year
  --to determine if the district upgraded bw
  LEFT JOIN ps.districts_sp_assignments sp
  ON fit.district_id = sp.district_id
  AND fit.funding_year = sp.funding_year
  --to determine if the district was eligible for upgrade last year
  LEFT JOIN ps.districts_fit_for_analysis fitpy
  ON fit.district_id = fitpy.district_id
  AND fit.funding_year - 1 = fitpy.funding_year
  --to determine if the district spend more this year
  LEFT JOIN ps.districts_bw_cost bcpy
  ON fit.district_id = bcpy.district_id
  AND fit.funding_year - 1 = bcpy.funding_year
  --to determine if the district submitted a form 470 for broadband
  LEFT JOIN (
    select distinct district_id
    from ps.districts_470s
    where num_broadband_470s > 0
  ) d470ay
  ON fit.district_id = d470ay.district_id
  where fit.funding_year = 2018
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
),

top_5_sp_not_meeting as (
  select
    primary_sp,
    sum(num_students) as num_students,
    rank() over (order by sum(num_students) desc) as rank
  from subset
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false
  and primary_sp is not null
  group by 1
),

meeting_not_meeting as (
  select
    case
      when sample_groups.meeting_2014_goal_no_oversub = false
        then 'not meeting goals'
      else 'meeting goals'
    end as subgroup,
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,-3) as num_students,
    round(sample_groups.num_students_top_5*sample_pop.population_students::numeric/sample_pop.sample_students,-3) as num_students_top_5,
    round(sample_groups.num_districts*sample_pop.population_districts::numeric/sample_pop.sample_districts,0) as num_districts,
    round(sample_groups.num_districts_could_meet*sample_pop.population_districts::numeric/sample_pop.sample_districts,0) as num_districts_could_meet,
    round(sample_groups.num_districts_no_470::numeric/sample_groups.num_districts,2) as pct_num_districts_no_470,
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
        sum(num_students) FILTER (WHERE top_5_sp.primary_sp is not null) as num_students_top_5,
        count(*) as num_districts,
        count(*) FILTER ( WHERE no_470_indicator = true) as num_districts_no_470,
        count(*) FILTER ( WHERE fit_for_ia = true
                          AND bandwidth_maximum >= bandwidth_needed_for_2014) as num_districts_could_meet,
        median(ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps,
        median(ia_bw_mbps_total) as median_ia_bw_mbps_total,
        median(ia_monthly_cost_per_mbps) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_per_mbps,
        median(ia_monthly_cost_total) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_total,
        median(ia_monthly_cost_total::numeric/num_students) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_per_student,
        sum(ia_monthly_cost_total)::numeric/sum(num_students) FILTER (WHERE fit_for_ia_cost = true) as agg_ia_monthly_cost_per_student
      from subset
      left join (
        select distinct primary_sp
        from top_5_sp_not_meeting
        where rank <= 5
      ) top_5_sp
      on subset.primary_sp = top_5_sp.primary_sp
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
        when upgrade_indicator = true
          then 'upgraded but not enough'
        when num_broadband_470s = 0
          then 'not looking to upgrade'
        when ia_frns_received_zero_bids = 0
          then 'looking to upgrade but cant afford bids'
        else 'looking to upgrade but no bids'
      end as subgroup,
      sum(num_students) as num_students,
      sum(num_students) FILTER (WHERE top_5_sp.primary_sp is not null) as num_students_top_5,
      count(*) as num_districts,
      count(*) FILTER ( WHERE no_470_indicator = true) as num_districts_no_470,
      count(*) FILTER ( WHERE fit_for_ia = true
                        AND bandwidth_maximum >= bandwidth_needed_for_2014) as num_districts_could_meet,
      median(ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps,
      median(ia_bw_mbps_total) as median_ia_bw_mbps_total,
      median(ia_monthly_cost_per_mbps) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_per_mbps,
      median(ia_monthly_cost_total) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_total,
      median(ia_monthly_cost_total::numeric/num_students) FILTER (WHERE fit_for_ia_cost = true) as median_ia_monthly_cost_per_student,
      sum(ia_monthly_cost_total)::numeric/sum(num_students) FILTER (WHERE fit_for_ia_cost = true) as agg_ia_monthly_cost_per_student
    from subset
    left join (
      select distinct primary_sp
      from top_5_sp_not_meeting
      where rank <= 5
    ) top_5_sp
    on subset.primary_sp = top_5_sp.primary_sp
    where fit_for_ia = true
    and fit_for_ia_py = true
    and meeting_2014_goal_no_oversub = false
    group by 1
  )

select
  sample_groups.subgroup,
  round(sample_groups.num_students*meeting_not_meeting.num_students::numeric/samples.num_students,-3) as num_students,
  round(sample_groups.num_students_top_5*meeting_not_meeting.num_students::numeric/samples.num_students,-3) as num_students_top_5,
  round(sample_groups.num_districts*meeting_not_meeting.num_districts::numeric/samples.num_districts,0) as num_districts,
  round(sample_groups.num_districts_could_meet*meeting_not_meeting.num_districts::numeric/samples.num_districts,0) as num_districts_could_meet,
  round(sample_groups.num_districts_no_470::numeric/sample_groups.num_districts,2) as pct_num_districts_no_470,
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
