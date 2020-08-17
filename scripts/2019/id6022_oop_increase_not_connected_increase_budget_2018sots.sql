with unnest_cck12_peers as (
  select distinct on (p.district_id, p.funding_year)
    p.district_id,
    p.funding_year,
    p.peer_id,
    bw.ia_annual_cost_total as peer_ia_annual_cost_total
  from (
    select 
      district_id,
      funding_year,
      unnest(bandwidth_suggested_districts) as peer_id
    from ps.districts_peers 
  ) p
  join ps.districts_bw_cost bw
  on p.peer_id = bw.district_id
  and p.funding_year = bw.funding_year
  join ps.districts d
  on p.peer_id = d.district_id
  and p.funding_year = d.funding_year
  join ps.districts_fit_for_analysis fit
  on p.peer_id = fit.district_id
  and p.funding_year = fit.funding_year
  where fit.fit_for_ia = true
  and fit.fit_for_ia_cost = true
  and d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
  order by p.district_id, p.funding_year, bw.ia_annual_cost_total desc
),

subset as (
  select 
    d.district_id,
    d.num_students,
    dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub,
    dbc.ia_monthly_cost_total as ia_monthly_cost_total,
    dbc.ia_annual_cost_total,
    dbc.ia_funding_requested_erate,
    dbc.meeting_knapsack_affordability_target as meeting_knapsack_affordability_target,
    dpr.district_id is not null as have_peer_deal,
    dfa.fit_for_ia_cost as fit_for_ia_cost,
    dfa.fit_for_ia as fit_for_ia,
    cp.peer_ia_annual_cost_total,
    case
      when d.c1_discount_rate is null 
        then .7
      else d.c1_discount_rate
    end as c1_discount_rate
    
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

  LEFT JOIN unnest_cck12_peers cp
  ON d.district_id = cp.district_id
  AND d.funding_year = cp.funding_year

  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
),

not_meeting as (
  select 
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,0) as num_students
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
    where sample_groups.meeting_2014_goal_no_oversub = false
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
    sum(num_students) as num_students,
    sum(case
          when ia_annual_cost_total != 0
            then (peer_ia_annual_cost_total*(ia_annual_cost_total-ia_funding_requested_erate)/ia_annual_cost_total) - 
                  (ia_annual_cost_total-ia_funding_requested_erate) 
          else peer_ia_annual_cost_total*(1-c1_discount_rate)
        end) as peer_oop_increase
  from subset
  where meeting_2014_goal_no_oversub = false
  and fit_for_ia_cost = true
  group by 1
)

select 
  round(sample_groups.peer_oop_increase*not_meeting.num_students::numeric/samples.num_students,-3) as peer_oop_increase
  from sample_groups
  join not_meeting
  on true
  join (
  select
    sum(num_students) as num_students
    from sample_groups
  ) samples
  on true
  where sample_groups.subgroup = 'increase budget' 