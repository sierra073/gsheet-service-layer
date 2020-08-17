  select 
    median(dbc.ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps_18,
    median(dbc2.ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps_17,
    median(dbc.ia_bandwidth_per_student_kbps-dbc2.ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps_change,
    median(dbc2.ia_monthly_cost_total) as median_ia_monthly_cost_total_17,
    median(dbc.ia_monthly_cost_total) as median_ia_monthly_cost_total_18,
    median(dbc.ia_monthly_cost_total-dbc2.ia_monthly_cost_total) as median_ia_monthly_cost_total_change
    
  from ps.districts d

  left join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year

  left join ps.districts_bw_cost dbc2
  on d.district_id= dbc2.district_id
  and d.funding_year = dbc2.funding_year + 1

  left join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year

  left join ps.districts_fit_for_analysis dfa2
  on d.district_id= dfa2.district_id
  and d.funding_year = dfa2.funding_year + 1

  left join (
    select distinct
      funding_year, 
      district_id
    from ps.districts_peers_ranks
  ) dpr2
  on d.district_id= dpr2.district_id
  and d.funding_year = dpr2.funding_year + 1

  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
  and dbc2.meeting_2014_goal_no_oversub = false
  and dbc.meeting_2014_goal_no_oversub = true
  and dfa.fit_for_ia_cost = true
  and dfa2.fit_for_ia_cost = true
  and dbc.ia_monthly_cost_total <= dbc2.ia_monthly_cost_total
  and (dbc.meeting_knapsack_affordability_target = true or dpr2.district_id is not null) 
