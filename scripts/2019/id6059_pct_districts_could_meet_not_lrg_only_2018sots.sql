with subset as (
  select 
    d.district_id,
    d.num_students,
    dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub,
    dbc.ia_monthly_cost_total as ia_monthly_cost_total,
    dbc.meeting_knapsack_affordability_target as meeting_knapsack_affordability_target,
    dpr.district_id is not null as have_peer_deal,
    dfa.fit_for_ia_cost as fit_for_ia_cost,
    dfa.fit_for_ia as fit_for_ia,
    --determined after discussing with jason and brian
    dl.fiber_internet_upstream_lines * 100000 +
    dl.fixed_wireless_internet_upstream_lines * 1000 +
    dl.cable_internet_upstream_lines * 1000 +
    dl.copper_internet_upstream_lines * 100 + 
    dl.satellite_lte_internet_upstream_lines * 40 as bandwidth_maximum,
    (d.num_students * .1) as bandwidth_needed_for_2014
    
  from ps.districts d

  left join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year

  left join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year

  --to determine the districts num internet lines
  JOIN ps.districts_lines dl
  ON d.district_id = dl.district_id
  AND d.funding_year = dl.funding_year

  left join (
    select distinct
      funding_year, 
      district_id
    from ps.districts_peers_ranks
  ) dpr
  on d.district_id= dpr.district_id
  and d.funding_year = dpr.funding_year

  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
)

  select 
    round(sum(1::numeric) FILTER ( WHERE bandwidth_maximum >= bandwidth_needed_for_2014)/
            count(*),2) as pct_districts_could_meet
  from subset
  where meeting_2014_goal_no_oversub = false
  and fit_for_ia = true
  and num_students <= 9000

