with subset as (
  select 
    d.district_id,
    d.state_code,
    d.num_schools,
    d.num_campuses,
    d.num_students,
    dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub,
    dfa.fit_for_ia as fit_for_ia,
    df.fiber_target_status,
    df.assumed_scalable_campuses + df.known_scalable_campuses as scalable_campuses

  from ps.districts d

  left join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year

  left join ps.districts_fiber df
  on d.district_id= df.district_id
  and d.funding_year = df.funding_year

  left join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year

  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
)

  select 
    sum(case
          when meeting_2014_goal_no_oversub = true
          and fiber_target_status = 'Not Target'
            then num_schools::numeric
          else 0
        end)/sum(num_schools) as pct_schools_meeting_100k_and_fiber_not_target,
    sum(case
          when meeting_2014_goal_no_oversub = true
            then scalable_campuses::numeric
          else 0
        end)/sum(num_campuses) as pct_campuses_meeting_100k_and_scalable
  from subset
  where fit_for_ia = true
