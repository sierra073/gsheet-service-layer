with subset as (
select 
  d.district_id,
  d.name,
  d.state_code,
  d.num_students,
  dfa.fit_for_ia,
  dfa.fit_for_ia_cost,
  dbc.meeting_2014_goal_no_oversub,
  dbc.ia_bw_mbps_total as ia_bw_mbps_total_17,
  dbc.ia_monthly_cost_per_mbps as ia_monthly_cost_per_mbps_17,
  dbc.ia_monthly_cost_total as ia_monthly_cost_total_17,
  dfa2.fit_for_ia_cost as fit_for_ia_cost_18,
  dbc2.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_18,
  dbc2.ia_bw_mbps_total as ia_bw_mbps_total_18,
  dbc2.ia_monthly_cost_per_mbps as ia_monthly_cost_per_mbps_18,
  dbc2.ia_monthly_cost_total as ia_monthly_cost_total_18,
  
  max(peer_ia_monthly_cost_per_mbps) as max_peer_price,
  
  min(peer_distance) as min_peer_distance,
  
  count(dpr.district_id) > 0 as had_peer_deal
  
from ps.districts d

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_bw_cost dbc2
on d.district_id= dbc2.district_id
and d.funding_year = dbc2.funding_year - 1

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

join ps.districts_fit_for_analysis dfa2
on d.district_id= dfa2.district_id
and d.funding_year = dfa2.funding_year - 1

join ps.districts_upgrades  du
on d.district_id= du.district_id
and d.funding_year = du.funding_year - 1

left join ps.districts_peers dp
on d.district_id= dp.district_id
and d.funding_year = dp.funding_year

left join ps.districts_peers_ranks dpr
on d.district_id= dpr.district_id
and d.funding_year = dpr.funding_year

where d.funding_year = 2018
and d.in_universe = true
and d.district_type = 'Traditional'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 order by 1),


final_sample as (
select 
  district_id, name, state_code, num_students,
  case when ia_monthly_cost_total_18 <= ia_monthly_cost_total_17 then true
    else false end as upgraded_no_total_cost_increase,
  ia_monthly_cost_total_17,
  ia_monthly_cost_total_18,
  (ia_monthly_cost_total_18 - ia_monthly_cost_total_17)::numeric/ia_monthly_cost_total_17 as total_cost_change_pct,
  ia_bw_mbps_total_17,
  ia_bw_mbps_total_18,
  (ia_bw_mbps_total_18)::numeric/ia_bw_mbps_total_17 as total_bw_change_x
  
  from subset
  
  where meeting_2014_goal_no_oversub = false
  and meeting_2014_goal_no_oversub_18 = true
  and fit_for_ia_cost = true
  and fit_for_ia_cost_18 = true
  and ia_monthly_cost_total_17 > 0
  and ia_bw_mbps_total_17 > 0
)

select * from final_sample order by upgraded_no_total_cost_increase desc