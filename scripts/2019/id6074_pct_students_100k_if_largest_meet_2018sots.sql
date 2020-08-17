with subset as (
  select 
    d.district_id,
    d.state_code,
    d.num_schools,
    d.num_students,
    dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub,
    dfa.fit_for_ia as fit_for_ia

  from ps.districts d

  left join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year

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
            then num_students::numeric
          else 0
        end)/sum(num_students) as pct_students_meeting_100k,
    sum(case
          when meeting_2014_goal_no_oversub = true
          or num_students > 9000
            then num_students::numeric
          else 0
        end)/sum(num_students) as pct_students_meeting_100k_given_63_largest_meet

  from subset
  where fit_for_ia = true