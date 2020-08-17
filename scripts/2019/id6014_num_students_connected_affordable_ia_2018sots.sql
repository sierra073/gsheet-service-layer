with subset as (
  select 
    d.district_id,
    d.num_students,
    dbc2.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_17,
    dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_18,
    dbc2.ia_monthly_cost_total as ia_monthly_cost_total_17,
    dbc.ia_monthly_cost_total as ia_monthly_cost_total_18,
    dbc.meeting_knapsack_affordability_target as meeting_knapsack_affordability_target_18,
    dpr2.district_id is not null as had_peer_deal,
    dfa2.fit_for_ia_cost as fit_for_ia_cost_17,
    dfa.fit_for_ia_cost as fit_for_ia_cost_18,
    dfa2.fit_for_ia as fit_for_ia_17,
    dfa.fit_for_ia as fit_for_ia_18 
    
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
),

newly_meeting as (
  select 
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,0) as num_students
    from (
      select 
        case
          when meeting_2014_goal_no_oversub_18 = true
          and meeting_2014_goal_no_oversub_17 = false
            then true
          else false
        end as newly_meeting_18,
        sum(num_students) as num_students
      from subset
      where fit_for_ia_18 = true
      and fit_for_ia_17 = true
      group by 1
    ) sample_groups
    join (
      select 
        sum(num_students) as population_students,
        sum(num_students) FILTER (WHERE fit_for_ia_18 = true
                                  AND fit_for_ia_17 = true) as sample_students
      from subset
    ) sample_pop  
    on true
    where sample_groups.newly_meeting_18 = true
),

sample_groups as (
  select 
    case 
            --not paying more and either had a peer deal to get them there or is meeting benchmark affordability
      when  ia_monthly_cost_total_18 <= ia_monthly_cost_total_17
            and (meeting_knapsack_affordability_target_18 = true or had_peer_deal = true) 
              then true 
      else false
    end as affordable_ia,
    sum(num_students) as num_students
  from subset
  where meeting_2014_goal_no_oversub_17 = false
  and meeting_2014_goal_no_oversub_18 = true
  and fit_for_ia_cost_17 = true
  and fit_for_ia_cost_18 = true
  group by 1
)

select 
  round(sample_groups.num_students*newly_meeting.num_students::numeric/samples.num_students,-5) as num_students
  from sample_groups
  join newly_meeting
  on true
  join (
  select
    sum(num_students) as num_students
    from sample_groups
  ) samples
  on true
  where sample_groups.affordable_ia = true