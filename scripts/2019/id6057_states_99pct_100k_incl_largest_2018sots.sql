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
),

states as (
  select 
    state_code,
    sum(case
          when meeting_2014_goal_no_oversub = true
            then num_schools::numeric
          else 0
        end)/sum(num_schools) as pct_schools_meeting_100k,
    sum(case
          when meeting_2014_goal_no_oversub = true
          or num_students > 9000
            then num_schools::numeric
          else 0
        end)/sum(num_schools) as pct_schools_meeting_100k_given_63_largest_meet,
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
  and state_code != 'DC'
  group by 1
)

  select
    'given 63 largest meet' as category, 
    'schools' as basis,
    count(state_code) as num_states_99pct_schools_meeting_100k
  from states
  where round(pct_schools_meeting_100k_given_63_largest_meet,2) >= .99

UNION

  select
    'current' as category,  
    'schools' as basis,
    count(state_code) as num_states_99pct_schools_meeting_100k
  from states
  where round(pct_schools_meeting_100k,2) >= .99

UNION

  select
    'given 63 largest meet' as category, 
    'students' as basis,
    count(state_code) as num_states_99pct_students_meeting_100k
  from states
  where round(pct_students_meeting_100k_given_63_largest_meet,2) >= .99

UNION

  select
    'current' as category,  
    'students' as basis,
    count(state_code) as num_states_99pct_students_meeting_100k
  from states
  where round(pct_students_meeting_100k,2) >= .99