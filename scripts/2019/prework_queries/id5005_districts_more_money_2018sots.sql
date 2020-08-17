--districts who havent seen a cost decrease and are not meeting knapsack
            
select 
  d.district_id, 
  d.state_code,
  d.name,
  d.funding_year, 
  d.latitude, 
  d.longitude, 
  d.num_students,
  d.locale,
  d.size,
  dbc.ia_monthly_cost_per_mbps,
  dbc.ia_monthly_cost_total::numeric/d.num_students as ia_monthly_cost_per_student,
  
  case when du.switcher = true 
      then false
    when  du.switcher = false
      then true
  end as same_sp,
  sp.primary_sp,
 
  case when dl.most_recent_ia_contract_end_date = dl2.most_recent_ia_contract_end_date
    then true
  else false end as same_contract_end_indicator,
  
  case when df.hierarchy_ia_connect_category != 'Fiber'
    then true
  else false end as non_fiber_hierarchy,
  case when df.assumed_unscalable_campuses + known_scalable_campuses > 0
    then true
  else false end as unscalable_campuses,
  
  case when dp.bandwidth_suggested_districts is null 
    then true
  else false end as no_peer_deal,
  
  case when dl.ia_frns_received_zero_bids > 0
    then true
  else false end as frns_received_0_bids,
  
  case when d470.num_broadband_470s > 0
    then true
  else false end as filed_470,
  
  case when dbc.ia_monthly_cost_per_mbps >= dbc2.ia_monthly_cost_per_mbps
  and dbc.meeting_knapsack_affordability_target = false
    then true
  else false end as no_decrease_no_aff,
  
  du.upgrade_indicator,
  case when duall.n_upgraded = 0 then true else false end as upgraded_0x

from ps.districts d

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

join ps.districts_fit_for_analysis dfa2
on d.district_id= dfa2.district_id
and d.funding_year = dfa2.funding_year + 1

join ps.districts_fiber df
on d.district_id= df.district_id
and d.funding_year = df.funding_year

join ps.districts_sp_assignments sp
on d.district_id= sp.district_id
and d.funding_year = sp.funding_year

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_bw_cost dbc2
on d.district_id= dbc2.district_id
and d.funding_year = dbc2.funding_year + 1

join ps.districts_lines dl
on d.district_id= dl.district_id
and d.funding_year = dl.funding_year

join ps.districts_lines dl2
on d.district_id= dl2.district_id
and d.funding_year = dl2.funding_year + 1

join ps.districts_470s d470
on d.district_id = d470.district_id
and d.funding_year = d470.funding_year

join ps.districts_upgrades  du
on d.district_id= du.district_id
and d.funding_year = du.funding_year

join 
(select district_id, count(case when upgrade_indicator = true then district_id end) as n_upgraded
from ps.districts_upgrades
group by 1)  duall
on d.district_id= duall.district_id

left join ps.districts_peers dp
on d.district_id= dp.district_id
and d.funding_year = dp.funding_year 

where d.funding_year = 2019
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and dfa2.fit_for_ia_cost = true
and d.state_code != 'AK'