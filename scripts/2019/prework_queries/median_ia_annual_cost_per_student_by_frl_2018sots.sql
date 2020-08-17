with subset as (
  select 
    d.district_id,
    d.num_students,
    d.c1_discount_rate,
    dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub,
    dbc.ia_monthly_cost_total as ia_monthly_cost_total,
    dbc.ia_annual_cost_total as ia_annual_cost_total,
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

meeting_not_meeting as (
  select 
    c1_discount_rate,
    case 
      when  meeting_2014_goal_no_oversub = true
        then 'meeting 2014 goal' 
      else 'not meeting 2014 goal'
    end as subgroup,
    median(ia_annual_cost_total/num_students) as median_ia_annual_cost_per_student
  from subset
  where fit_for_ia_cost = true
  and c1_discount_rate is not null
  group by 1,2
),

sample_groups as (
  select 
    c1_discount_rate,
    case 
            --either have a peer deal to get them what they need or are spending enough to get bw at benchmark affordability
      when  have_peer_deal = true
      or (case
              when ia_monthly_cost_total < 14*50 and ia_monthly_cost_total > 0
                then ia_monthly_cost_total/14
              else ps.knapsack_bandwidth(ia_monthly_cost_total)
            end*1000/num_students) >= 100
        then 'budget sufficient' 
      else 'increase budget'
    end as subgroup,
    median(ia_annual_cost_total/num_students) as median_ia_annual_cost_per_student
  from subset
  where meeting_2014_goal_no_oversub = false
  and fit_for_ia_cost = true
  and c1_discount_rate is not null
  group by 1,2
)

select 
  subgroup,
  c1_discount_rate,
  round(sample_groups.median_ia_annual_cost_per_student,2) as median_ia_annual_cost_per_student
from sample_groups

  UNION

select 
  subgroup,
  c1_discount_rate,
  round(meeting_not_meeting.median_ia_annual_cost_per_student,2) as median_ia_annual_cost_per_student
from meeting_not_meeting
where subgroup = 'meeting 2014 goal'