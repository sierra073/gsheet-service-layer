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
    d470.num_broadband_470s,
    dl.ia_frns_received_zero_bids,
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

  JOIN ps.districts_470s d470
  ON dfa.district_id = d470.district_id
  AND dfa.funding_year = d470.funding_year

  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
),

sample_groups as (
  select 
    case 
            --either have a peer deal to get them what they need or are spending enough to get bw at benchmark affordability
      when  have_peer_deal = true
        then 'budget sufficient at peer deal' 
      when (case
              when ia_monthly_cost_total < 14*50 and ia_monthly_cost_total > 0
                then ia_monthly_cost_total/14
              else ps.knapsack_bandwidth(ia_monthly_cost_total)
            end*1000/num_students) >= 100
        then 'budget sufficient at benchmark' 
      else 'increase budget'
    end as subgroup,
      case
        when num_broadband_470s = 0
          then 'not looking to upgrade'
        when ia_frns_received_zero_bids = 0
          then 'looking to upgrade but cant afford bids'
        when ia_frns_received_zero_bids > 0 then 'looking to upgrade but got 0 bids'
      end as subgroup_upgrade,
    count(*) FILTER ( WHERE bandwidth_maximum >= bandwidth_needed_for_2014) as num_districts_could_meet,
    count(*) as num_districts
  from subset
  where meeting_2014_goal_no_oversub = false
  and fit_for_ia_cost = true
  group by 1,2
)
  

select 
  round(sum(sample_groups.num_districts_could_meet)/sum(sample_groups.num_districts),2) as pct_districts_could_meet
  from sample_groups
  where sample_groups.subgroup in ('budget sufficient at peer deal', 'budget sufficient at benchmark' )
  and sample_groups.subgroup_upgrade = 'looking to upgrade but cant afford bids'