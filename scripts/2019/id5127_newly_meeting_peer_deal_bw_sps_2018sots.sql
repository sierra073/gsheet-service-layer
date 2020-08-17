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
  dbc.ia_monthly_cost_per_mbps as ia_monthly_cost_per_mbps_18,
  dbc.ia_monthly_cost_total as ia_monthly_cost_total_18,
  du.switcher,
  sp.primary_sp,
  
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

join ps.districts_sp_assignments sp
on dd.district_id= sp.district_id
and dd.funding_year = sp.funding_year

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

final_sample as (
select 
  district_id, name, state_code, num_students,
  primary_sp, ia_bw_mbps_total_18, ia_bw_mbps_total_17,
  had_peer_deal, meeting_2014_goal_no_oversub_18,
  case when ia_monthly_cost_total_18 <= ia_monthly_cost_total_17
  then true else false end as got_better_deal,
  switcher
  
  from subset
  
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false
  and fit_for_ia_18 = true
),

top_sps as (
select primary_sp,
ROW_NUMBER() OVER(ORDER BY num_students desc) as rank
from (
select primary_sp,
sum(num_students) as num_students
from final_sample
where had_peer_deal = true and got_better_deal = true and meeting_2014_goal_no_oversub_18 = true
group by 1) sp
)

select
sum(ia_bw_mbps_total_18)::numeric/sum(ia_bw_mbps_total_17) as bw_increase,
array_agg(distinct sps.primary_sp) as top4_sps
from final_sample
join (select * from top_sps where rank < 5 order by rank) sps
on true
where had_peer_deal = true and got_better_deal = true and meeting_2014_goal_no_oversub_18 = true