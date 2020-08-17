with subset as (
select 
  dd.district_id,
  dd.name,
  dd.state_code,
  dd.num_students,
  dd.locale,
  dd.size,
  dd.frl_percent,
  dfa2.fit_for_ia,
  dfa2.fit_for_ia_cost,
  dbc2.meeting_2014_goal_no_oversub,
  dbc2.ia_bw_mbps_total as ia_bw_mbps_total_17,
  dbc2.ia_monthly_cost_per_mbps as ia_monthly_cost_per_mbps_17,
  dbc2.ia_monthly_cost_total as ia_monthly_cost_total_17,
  dfa.fit_for_ia as fit_for_ia_18,
  dfa.fit_for_ia_cost as fit_for_ia_cost_18,
  dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_18,
  dbc.ia_bw_mbps_total as ia_bw_mbps_total_18,
  dbc.ia_bandwidth_per_student_kbps as ia_bandwidth_per_student_kbps_18,
  dbc.ia_monthly_cost_per_mbps as ia_monthly_cost_per_mbps_18,
  dbc.ia_monthly_cost_total as ia_monthly_cost_total_18,
  du.switcher,
  
  max(peer_ia_monthly_cost_per_mbps) as max_peer_price,
  
  min(peer_distance) as min_peer_distance,
  
  count(dpr.district_id) > 0 as had_peer_deal
  
from ps.districts dd

join ps.districts_bw_cost dbc
on dd.district_id= dbc.district_id
and dd.funding_year = dbc.funding_year

join ps.districts_bw_cost dbc2
on dd.district_id= dbc2.district_id
and dd.funding_year = dbc2.funding_year + 1

join ps.districts_fit_for_analysis dfa
on dd.district_id= dfa.district_id
and dd.funding_year = dfa.funding_year

join ps.districts_fit_for_analysis dfa2
on dd.district_id= dfa2.district_id
and dd.funding_year = dfa2.funding_year + 1

join ps.districts_upgrades  du
on dd.district_id= du.district_id
and dd.funding_year = du.funding_year 

left join ps.districts_peers dp
on dd.district_id= dp.district_id
and dd.funding_year = dp.funding_year + 1

left join ps.districts_peers_ranks dpr
on dd.district_id= dpr.district_id
and dd.funding_year = dpr.funding_year + 1

where dd.funding_year = 2019
and dd.in_universe = true
and dd.district_type = 'Traditional'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21 order by 1),

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
),

newly_meeting as (
  select 
    round(sample_groups.num_districts*sample_pop.population_districts::numeric/sample_pop.sample_districts,0)- 
       meeting_2018_pop.districts_meeting_100kbps 
        as num_districts,
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,0)- 
       meeting_2018_pop.students_meeting_100kbps 
        as num_students
    from (
      select 
        meeting_2014_goal_no_oversub_18,
        count(*) as num_districts,
        sum(num_students) as num_students
      from subset
      where fit_for_ia_18 = true
      group by 1
    ) sample_groups
    join (
      select 
        count(*) as population_districts,
        count(*) FILTER (WHERE fit_for_ia_18 = true) as sample_districts,
        sum(num_students) as population_students,
        sum(num_students) FILTER (WHERE fit_for_ia_18 = true) as sample_students
      from subset
    ) sample_pop  
    on true
    left join (
      select districts_meeting_100kbps,
      students_meeting_100kbps
      from ps.state_snapshot_frozen_sots
      where funding_year = 2018
      and state_code = 'ALL'
    ) meeting_2018_pop
    on true
    where sample_groups.meeting_2014_goal_no_oversub_18 = true
),

final_sample as (
select 
  district_id, name, state_code, num_students, locale, size, frl_percent,
  ia_bandwidth_per_student_kbps_18,
  had_peer_deal, meeting_2014_goal_no_oversub_18,
  case when ia_monthly_cost_total_18 <= ia_monthly_cost_total_17
  then true else false end as got_better_deal,
  switcher
  
  from subset
  
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false
  and fit_for_ia_18 = true
)

select
case when got_better_deal = true and had_peer_deal = true 
then '1. newly meeting(<= 200kbps): got peer deal at no cost'
when got_better_deal = true 
then '2. newly meeting(<= 200kbps): no cost' 
else '3. newly meeting(<= 200kbps): paid more' end as subgroup,
round((count(fs.district_id)::numeric/
  (select count(*) as sample_pop from final_sample where meeting_2014_goal_no_oversub_18 = true)) *
  --population newly meeting
  (select num_districts from newly_meeting)::numeric,0) as num_districts

from final_sample fs
where meeting_2014_goal_no_oversub_18 = true
and ia_bandwidth_per_student_kbps_18 <= 200

group by 1 

union

select
case when got_better_deal = true and had_peer_deal = true 
then '4. newly meeting: got peer deal at no cost'
when got_better_deal = true 
then '5. newly meeting: no cost' 
else '6. newly meeting: paid more' end as subgroup,
round((count(fs.district_id)::numeric/
  (select count(*) as sample_pop from final_sample where meeting_2014_goal_no_oversub_18 = true)) *
  --population newly meeting
  (select num_districts from newly_meeting)::numeric,0) as num_districts

from final_sample fs
where meeting_2014_goal_no_oversub_18 = true

group by 1 
order by 1