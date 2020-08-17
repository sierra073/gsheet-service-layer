--how many districts in 2018 (fit for cost) had a peer deal
select 
  d.district_id,
  case when dpr.district_id is not null
    then 1
  else 0 end as had_peer_deal,
  dbc.meeting_2014_goal_no_oversub,
  dbc.ia_bw_mbps_total as ia_bw_mbps_total_17,
  dbc.ia_monthly_cost_per_mbps as ia_monthly_cost_per_mbps_17,
  
  case when dbc2.ia_bw_mbps_total >= dbc.ia_bw_mbps_total 
  and dbc2.ia_monthly_cost_per_mbps < dbc.ia_monthly_cost_per_mbps
    then 1
  else 0 end as dec_cost_more_bw,
  
  array_agg(case when (dbc2.ia_bw_mbps_total >= dbc.ia_bw_mbps_total 
  and dbc2.ia_monthly_cost_per_mbps < dbc.ia_monthly_cost_per_mbps)
  and (round(dbc2.ia_monthly_cost_per_mbps,1) = round(dpr.peer_ia_monthly_cost_per_mbps,1))
  and (round(dbc2.ia_monthly_cost_per_mbps,1) = round(dpr.peer_ia_monthly_cost_per_mbps,1))
    then 1
  else 0 end) as got_exact_peer_deals
  
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
and d.funding_year = du.funding_year

left join ps.districts_peers dp
on d.district_id= dp.district_id
and d.funding_year = dp.funding_year

left join ps.districts_peers_ranks dpr
on d.district_id= dpr.district_id
and d.funding_year = dpr.funding_year

where d.funding_year = 2018
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and dfa2.fit_for_ia_cost = true
and d.state_code != 'AK'
group by 1,2,3,4,5,6 order by 1