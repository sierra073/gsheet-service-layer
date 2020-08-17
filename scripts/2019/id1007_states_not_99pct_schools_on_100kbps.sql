with districts as (

    select d.state_code,
    d.funding_year,
    d.district_id,
    d.size,
    d.num_students,
    d.num_campuses,
    d.num_schools,
    f.fiber_target_status,
    case
      when fit.fit_for_ia = true
        then bw.meeting_2014_goal_no_oversub
    end as meeting_2014_goal_no_oversub

    from ps.districts_fit_for_analysis fit

    inner join ps.districts d
    on d.district_id = fit.district_id
    and d.funding_year = fit.funding_year

    inner join ps.districts_wifi w
    on w.district_id = fit.district_id
    and w.funding_year = fit.funding_year

    inner join ps.districts_bw_cost bw
    on bw.district_id = fit.district_id
    and bw.funding_year = fit.funding_year

    inner join ps.districts_fiber f
    on f.district_id = fit.district_id
    and f.funding_year = fit.funding_year

    where d.district_type = 'Traditional'
    and d.in_universe = true
    and d.state_code != 'DC'
    and d.funding_year = 2019

),

states_pre as (select
  funding_year,
  state_code,
  array_agg(num_schools) FILTER (WHERE meeting_2014_goal_no_oversub = false) as num_schools_in_not_meeting,
  max(num_schools) FILTER (WHERE meeting_2014_goal_no_oversub = false) as largest_district_not_meeting,
  count(case when meeting_2014_goal_no_oversub = false then district_id end) as num_districts_not_meeting,
  count(case when meeting_2014_goal_no_oversub is not null then district_id end) as num_districts_pop,
  sum(case when meeting_2014_goal_no_oversub = false then num_students end) as num_students_not_meeting,
  sum(case when meeting_2014_goal_no_oversub is not null then num_students end) as num_students_pop,
  sum(case when meeting_2014_goal_no_oversub = false then num_schools end) as num_schools_not_meeting,
  sum(case when meeting_2014_goal_no_oversub is not null then num_schools end) as num_schools_pop,

  --connectivity
  round(case
    when sum(case
          when meeting_2014_goal_no_oversub is not null
            then num_schools
          end) = 0
      then 0
    else
        sum(case
          when meeting_2014_goal_no_oversub = true
            then num_schools
          end)::numeric/
              sum(case
                when meeting_2014_goal_no_oversub is not null
                  then num_schools
                end)
  end,2) as connectivity_schools,
  --Identifying what would be the minimum # of schools they would need left
  --to convert to hit 98.5% (which rounds to 99%), therefore multiplying by .015
  (sum(case
    when meeting_2014_goal_no_oversub is not null
        then num_schools
        end) * .015) as num_schools_left_to_meet,
  round(case
    when sum(case
          when meeting_2014_goal_no_oversub is not null
            then num_students
          end) = 0
      then 0
    else
        sum(case
          when meeting_2014_goal_no_oversub = true
            then num_students
          end)::numeric/
              sum(case
                when meeting_2014_goal_no_oversub is not null
                  then num_students
                end)
  end,2) as connectivity_students


  from districts

  group by funding_year,
  state_code

),

states as (
  SELECT
        state_code,
        num_schools_in_not_meeting,
        largest_district_not_meeting,
        ceiling(num_schools_not_meeting - num_schools_left_to_meet) as num_schools_needed,
        CASE
          WHEN ceiling(num_schools_not_meeting - num_schools_left_to_meet) <= largest_district_not_meeting
          THEN TRUE ELSE FALSE
        END AS only_largest_districts_needs_upgrade,
        num_districts_not_meeting,
        num_districts_pop,
        num_students_not_meeting,
        num_students_pop,
        num_schools_not_meeting,
        num_schools_pop,
        SUM(CASE
              WHEN funding_year = 2019
                then connectivity_schools
              ELSE 0
            END) as pct_100kbps_2019_schools,
        SUM(CASE
              WHEN funding_year = 2019
                then connectivity_students
              ELSE 0
            END) as pct_100kbps_2019_students
  FROM states_pre

  GROUP BY state_code,
  num_schools_in_not_meeting,
  num_districts_not_meeting,
  largest_district_not_meeting,
  only_largest_districts_needs_upgrade,
  num_districts_pop,
  num_students_not_meeting,
  num_students_pop,
  num_schools_not_meeting,
  num_schools_pop,
  num_schools_left_to_meet
)

select *
from states
where
  pct_100kbps_2019_schools < .99

order by
  pct_100kbps_2019_schools desc,
  pct_100kbps_2019_students >= .99 desc,
  state_code
