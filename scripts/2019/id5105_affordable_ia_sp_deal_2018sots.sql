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
    fitpy.fit_for_ia as fit_for_ia_py,
    fitpy.fit_for_ia_cost as fit_for_ia_cost_py,
    dbc.ia_bandwidth_per_student_kbps,
    dbc.ia_bw_mbps_total,
    dbc.ia_monthly_cost_per_mbps,
    d470.num_broadband_470s,
    up.upgrade_indicator,
    dl.ia_frns_received_zero_bids,
    dl.most_recent_ia_contract_end_date,
    d.c1_discount_rate,
    d470ay.district_id is null as no_470_indicator,
    sp.primary_sp
    
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
  
  JOIN ps.districts_470s d470
  ON dfa.district_id = d470.district_id
  AND dfa.funding_year = d470.funding_year
  --to determine if the district submitted a form 471 for internet and received 0 bids
  JOIN ps.districts_lines dl
  ON dfa.district_id = dl.district_id
  AND dfa.funding_year = dl.funding_year
  --to determine if the district upgraded bw
  JOIN ps.districts_upgrades up
  ON dfa.district_id = up.district_id
  AND dfa.funding_year = up.funding_year
  --to determine if the district upgraded bw
  --to determine if the district was eligible for upgrade last year
  LEFT JOIN ps.districts_fit_for_analysis fitpy
  ON dfa.district_id = fitpy.district_id
  AND dfa.funding_year - 1 = fitpy.funding_year
  --to determine if the district spend more this year
  LEFT JOIN ps.districts_bw_cost bcpy
  ON dfa.district_id = bcpy.district_id
  AND dfa.funding_year - 1 = bcpy.funding_year
  --to determine if the district submitted a form 470 for broadband
  LEFT JOIN (
    select distinct district_id
    from ps.districts_470s 
    where num_broadband_470s > 0
  ) d470ay
  ON dfa.district_id = d470ay.district_id
  LEFT JOIN ps.districts_sp_assignments sp
  on dfa.district_id = sp.district_id
  and dfa.funding_year = sp.funding_year
  
  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
),

sp_deals as (
select 
    sp.primary_sp,
    dbc.ia_monthly_cost_total as ia_monthly_cost_total,
    dbc.ia_bw_mbps_total,
    dbc.ia_monthly_cost_per_mbps
    
    from ps.districts d
    join ps.districts_bw_cost dbc
    on d.district_id= dbc.district_id
    and d.funding_year = dbc.funding_year
    join ps.districts_fit_for_analysis dfa
    on d.district_id= dfa.district_id
    and d.funding_year = dfa.funding_year
    join ps.districts_sp_assignments sp
    on dfa.district_id = sp.district_id
    and dfa.funding_year = sp.funding_year
    
    where d.funding_year = 2019
    and d.in_universe = true
    and d.district_type = 'Traditional'
    and dfa.fit_for_ia = true
    and dfa.fit_for_ia_cost = true
    --subset to those meeting
    and meeting_2014_goal_no_oversub = true
    and sp.primary_sp is not null
    group by 1,2,3,4
),

sample_groups1 as (
  select 
  district_id,
  upgrade_indicator,
  num_broadband_470s,
  ia_frns_received_zero_bids,
    case 
            --either have a peer deal to get them what they need or are spending enough to get bw at benchmark affordability
      when  have_peer_deal = true
        then 'budget sufficient at peer deal' 
      when (case
              when subset.ia_monthly_cost_total < 14*50 and subset.ia_monthly_cost_total > 0
                then subset.ia_monthly_cost_total/14
              else ps.knapsack_bandwidth(subset.ia_monthly_cost_total)
            end*1000/num_students) >= 100
        then 'budget sufficient at benchmark' 
      else 'increase budget'
    end as subgroup,
    num_students,
  
  max(case when (sp_deals.ia_bw_mbps_total >= subset.num_students * .1)
    and (sp_deals.ia_monthly_cost_total <= subset.ia_monthly_cost_total)
    then 1
  else 0 end) > 0 as sp_offering_deal_nationally
  
  from subset
  left join sp_deals
  on subset.primary_sp = sp_deals.primary_sp
  
  where meeting_2014_goal_no_oversub = false
  and fit_for_ia_cost = true
  group by 1,2,3,4,5,6
),

sample_groups as (
    select
     district_id,
     num_students,
     sp_offering_deal_nationally,
     case when subgroup != 'increase budget'
    then 'affordable_ia' 
    else subgroup end as subgroup,
      case
        when num_broadband_470s = 0
          then 'not looking to upgrade'
        when ia_frns_received_zero_bids = 0
          then 'looking to upgrade but cant afford bids'
        when ia_frns_received_zero_bids > 0 then 'looking to upgrade but got 0 bids'
      end as subgroup_upgrade
    from sample_groups1
  ),
  
final as (
select * from sample_groups
where subgroup = 'affordable_ia'
and subgroup_upgrade = 'looking to upgrade but cant afford bids'
),

not_meeting as (
  select 
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,0) as num_students,
    round(sample_groups.num_districts*sample_pop.population_districts::numeric/sample_pop.sample_districts,0) as num_districts
    from (
      select 
        meeting_2014_goal_no_oversub,
        sum(num_students) as num_students,
        count(*) as num_districts
      from subset
      where fit_for_ia = true
      group by 1
    ) sample_groups
    join (
      select 
        sum(num_students) as population_students,
        sum(num_students) FILTER (WHERE fit_for_ia = true) as sample_students,
        count(*) as population_districts,
        count(*) FILTER (WHERE fit_for_ia = true) as sample_districts
      from subset
    ) sample_pop  
    on true
    where sample_groups.meeting_2014_goal_no_oversub = false
)

select
  sp_offering_deal_nationally = true, 

  --percentage of districts where sp offers deal out of final sample
  round((sum(final.num_students)::numeric/(select sum(num_students) from final)) *
  --percentage of final sample out of not meeting, fit for cost
  ((select sum(num_students) from final)::numeric/
  (select sum(num_students) from subset 
  where fit_for_ia_cost = true
  and meeting_2014_goal_no_oversub = false)) *
  --percentage not meeting, clean for cost 
  ((select sum(num_students) from subset 
  where fit_for_ia_cost = true
  and meeting_2014_goal_no_oversub = false)::numeric/
  (select sum(num_students) from subset)) *
  --population 
  (select sum(num_students) from subset)::numeric,0) as num_students_sp_offering_deal_nationally,

  --percentage of districts where sp offers deal out of out of not meeting, fit for cost
  round((sum(final.num_students)::numeric/
  (select sum(num_students) from subset 
  where fit_for_ia_cost = true
  and meeting_2014_goal_no_oversub = false)) *
  --population not meeting 
  (select num_students from not_meeting)::numeric,0) as num_students_new_extrap,


  --percentage of districts where sp offers deal out of final sample
  round((count(final.district_id)::numeric/(select count(district_id) from final)) *
  --percentage of final sample out of not meeting, fit for cost
  ((select count(district_id) from final)::numeric/
  (select count(district_id) from subset 
  where fit_for_ia_cost = true
  and meeting_2014_goal_no_oversub = false)) *
  --percentage not meeting, clean for cost 
  ((select count(district_id) from subset 
  where fit_for_ia_cost = true
  and meeting_2014_goal_no_oversub = false)::numeric/
  (select count(district_id) from subset)) *
  --population 
  (select count(district_id) from subset)::numeric,0) as num_districts_sp_offering_deal_nationally,

  --percentage of districts where sp offers deal out of out of not meeting, fit for cost
  round((count(final.district_id)::numeric/
  (select count(*) from subset 
  where fit_for_ia_cost = true
  and meeting_2014_goal_no_oversub = false)) *
  --population not meeting 
  (select num_districts from not_meeting)::numeric,0) as num_districts_new_extrap


from final
group by 1