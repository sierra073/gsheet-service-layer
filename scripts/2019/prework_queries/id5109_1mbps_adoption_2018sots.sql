select d.funding_year,
  (sum(case when dbc.meeting_2018_goal_oversub = true then d.num_students end)::numeric/sum(d.num_students)) as num_students
  
  from ps.districts d
  join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year
  join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year
  
  where 
  d.funding_year = 2015
  and district_type = 'Traditional'
  and fit_for_ia = true
  group by 1
  
  union
  
  select d.funding_year,
  (sum(case when dbc.meeting_2018_goal_oversub = true then d.num_students end)::numeric/sum(d.num_students)) as num_students
  
  from ps.districts d
  join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year
  join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year
  
  /*join ps.districts_bw_cost dprev
  on d.district_id= dprev.district_id
  and d.funding_year = dprev.funding_year + 1
  join ps.districts_fit_for_analysis dfprev
  on d.district_id= dfprev.district_id
  and d.funding_year = dfprev.funding_year + 1*/
  
  where 
  d.funding_year = 2017
  and district_type = 'Traditional'
  and dfa.fit_for_ia = true
  /*and dfprev.fit_for_ia = true*/
  group by 1
  
  union
  
  select d.funding_year,
  (sum(case when dbc.meeting_2018_goal_oversub = true then d.num_students end)::numeric/sum(d.num_students)) as num_students
  
  from ps.districts d
  join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year
  join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year
  
  /*join ps.districts_bw_cost dprev
  on d.district_id= dprev.district_id
  and d.funding_year = dprev.funding_year + 1
  join ps.districts_fit_for_analysis dfprev
  on d.district_id= dfprev.district_id
  and d.funding_year = dfprev.funding_year + 1*/
  
  where 
  d.funding_year = 2018
  and district_type = 'Traditional'
  and dfa.fit_for_ia = true
  /*and dfprev.fit_for_ia = true*/
  group by 1
  
  union
  
  select d.funding_year,
  (sum(case when dbc.meeting_2018_goal_oversub = true then d.num_students end)::numeric/sum(d.num_students)) as num_students
  
  from ps.districts d
  join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year
  join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year
  
  /*join ps.districts_bw_cost dprev
  on d.district_id= dprev.district_id
  and d.funding_year = dprev.funding_year + 1
  join ps.districts_fit_for_analysis dfprev
  on d.district_id= dfprev.district_id
  and d.funding_year = dfprev.funding_year + 1*/
  
  where 
  d.funding_year = 2019
  and in_universe = true
  and district_type = 'Traditional'
  and dfa.fit_for_ia = true
  /*and dfprev.fit_for_ia = true*/
  group by 1 order by 1