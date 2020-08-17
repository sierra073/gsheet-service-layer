with subset as (
  select 
    d.district_id,
    d.num_students,
    dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub,
    dbc.ia_monthly_cost_total as ia_monthly_cost_total,
    dbc.meeting_knapsack_affordability_target as meeting_knapsack_affordability_target,
    dpr.district_id is not null as have_peer_deal,
    dfa.fit_for_ia_cost as fit_for_ia_cost,
    dfa.fit_for_ia as fit_for_ia 
    
  from ps.districts d

  left join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year

  left join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year

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
    sum(num_students) as num_students
  from subset
  where meeting_2014_goal_no_oversub = false
  and fit_for_ia_cost = true
  group by 1
)

select 
  round(sum(sample_groups.num_students)/samples.num_students,2) as pct_students
  from sample_groups
  join (
  select
    sum(num_students) as num_students
    from sample_groups
  ) samples
  on true
  where sample_groups.subgroup in ('budget sufficient at peer deal', 'budget sufficient at benchmark' )
  group by samples.num_students
