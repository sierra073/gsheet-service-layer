with subset as (
  select 
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    fit.fit_for_ia,
    fit.fit_for_ia_cost,
    d470.num_broadband_470s,
    dd.num_students,
    d470ay.district_id is null as no_470_indicator,
    dlpy.most_recent_ia_contract_end_date as most_recent_ia_contract_end_date_py,
    dl.most_recent_ia_contract_end_date as most_recent_ia_contract_end_date_cy,
    dd.technology_contact
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
  AND fit.funding_year = d470.funding_year
  --to determine if the district submitted a form 470 for broadband in any year
  LEFT JOIN (
    select distinct district_id
    from ps.districts_470s 
    where num_broadband_470s > 0
  ) d470ay
  ON fit.district_id = d470ay.district_id
  --to determine if last year contract expiring
  left JOIN ps.districts_lines dlpy
  ON fit.district_id = dlpy.district_id
  AND fit.funding_year-1 = dlpy.funding_year
  --to determine if contract expiring
  left JOIN ps.districts_lines dl
  ON fit.district_id = dl.district_id
  AND fit.funding_year = dl.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
),

meeting_not_meeting as (
  select 
    meeting_2014_goal_no_oversub,
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,0)
        as num_students
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
)

    select 
      subset.meeting_2014_goal_no_oversub,
      sum(1::numeric) filter (where not(technology_contact = true)) / count(*) 
        as pct_districts_no_technology_contact,
      sum(1::numeric) filter (where num_broadband_470s = 0) / count(*) 
        as pct_districts_no_470_indicator_2019,
      sum(1::numeric) filter (where num_broadband_470s = 0
                              and not(technology_contact = true)) / 
        sum(1::numeric) filter (where num_broadband_470s = 0) 
          as pct_districts_no_470_indicator_2019_with_no_technology_contact,
      sum(1::numeric) filter (where no_470_indicator = true) / count(*) 
        as pct_districts_no_470_indicator_2015,
      sum(1::numeric) filter (where no_470_indicator = true
                              and not(technology_contact = true)) / 
        sum(1::numeric) filter (where no_470_indicator = true) 
          as pct_districts_no_470_indicator_2015_with_no_technology_contact,
      sum(1::numeric) filter (where most_recent_ia_contract_end_date_py <= '2019-06-30'::date
                              and num_broadband_470s = 0) / 
        count(*) filter ( where most_recent_ia_contract_end_date_py is not null
                          and num_broadband_470s = 0) 
          as pct_districts_no_470_2019_with_contract_expiring_2019,
      sum(1::numeric) filter (where most_recent_ia_contract_end_date_cy <= '2019-06-30'::date
                              and num_broadband_470s = 0) / 
        count(*) filter ( where most_recent_ia_contract_end_date_cy is not null
                          and num_broadband_470s = 0) 
          as pct_districts_no_470_2019_with_contract_expiring_2019,
      sum(subset.num_students::numeric) filter ( 
          where most_recent_ia_contract_end_date_cy <= '2019-06-30'::date
          and num_broadband_470s = 0) / 
        sum(subset.num_students::numeric) filter ( 
          where most_recent_ia_contract_end_date_cy is not null
          and num_broadband_470s = 0) *
            meeting_not_meeting.num_students as num_students_no_470_2019_with_contract_expiring_2019,
      sum(1::numeric) filter (where most_recent_ia_contract_end_date_cy <= '2020-06-30'::date
                              and num_broadband_470s = 0) / 
        count(*) filter ( where most_recent_ia_contract_end_date_cy is not null
                          and num_broadband_470s = 0) 
          as pct_districts_no_470_2019_with_contract_expiring_2020,
      sum(1::numeric) filter (where most_recent_ia_contract_end_date_cy <= '2021-06-30'::date
                              and num_broadband_470s = 0) / 
        count(*) filter ( where most_recent_ia_contract_end_date_cy is not null
                          and num_broadband_470s = 0) 
          as pct_districts_no_470_2019_with_contract_expiring_2021,
      sum(1::numeric) filter (where most_recent_ia_contract_end_date_cy <= '2022-06-30'::date
                              and num_broadband_470s = 0) / 
        count(*) filter ( where most_recent_ia_contract_end_date_cy is not null
                          and num_broadband_470s = 0) 
          as pct_districts_no_470_2019_with_contract_expiring_2022,
      sum(1::numeric) filter (where most_recent_ia_contract_end_date_cy <= '2023-06-30'::date
                              and num_broadband_470s = 0) / 
        count(*) filter ( where most_recent_ia_contract_end_date_cy is not null
                          and num_broadband_470s = 0) 
          as pct_districts_no_470_2019_with_contract_expiring_2023,
      sum(1::numeric) filter (where most_recent_ia_contract_end_date_cy <= '2024-06-30'::date
                              and num_broadband_470s = 0) / 
        count(*) filter ( where most_recent_ia_contract_end_date_cy is not null
                          and num_broadband_470s = 0) 
          as pct_districts_no_470_2019_with_contract_expiring_2024,
      median(date_part('year',age(most_recent_ia_contract_end_date_cy,'2019-06-30'::date))::numeric) 
        filter (where most_recent_ia_contract_end_date_cy > '2019-06-30'::date
                and num_broadband_470s = 0) as median_ced_no_470_2019_no_contract_expiring_cy
    from subset
    join meeting_not_meeting
    on subset.meeting_2014_goal_no_oversub = meeting_not_meeting.meeting_2014_goal_no_oversub
    where fit_for_ia = true
    group by 1, meeting_not_meeting.num_students