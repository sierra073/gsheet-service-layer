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
  sp.primary_sp,
  dbc.ia_annual_cost_erate,
  dbc.ia_funding_requested_erate,
  case when dbc2.ia_monthly_cost_per_mbps > 0
  then (dbc.ia_monthly_cost_per_mbps - dbc2.ia_monthly_cost_per_mbps)/dbc2.ia_monthly_cost_per_mbps::numeric
  end as pchg_cost_mbps,
  
  case when dbc2.ia_monthly_cost_per_mbps > dbc.ia_monthly_cost_per_mbps
    then true
  else false end as cost_decrease_indicator,
 
  case when dl.most_recent_ia_contract_end_date > dl2.most_recent_ia_contract_end_date
    then true
  else false end as changed_contract_indicator,
  
  case when dp.bandwidth_suggested_districts is not null 
    then true
  else false end as had_peer_deal_indicator,
  
  du.switcher as switched_sp,
  du.added_fiber

from ps.districts d

join ps.districts_sp_assignments sp
on d.district_id= sp.district_id
and d.funding_year = sp.funding_year

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_bw_cost dbc2
on d.district_id= dbc2.district_id
and d.funding_year = dbc2.funding_year + 1

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

join ps.districts_fit_for_analysis dfa2
on d.district_id= dfa2.district_id
and d.funding_year = dfa2.funding_year + 1

join ps.districts_lines dl
on d.district_id= dl.district_id
and d.funding_year = dl.funding_year

join ps.districts_lines dl2
on d.district_id= dl2.district_id
and d.funding_year = dl2.funding_year + 1

join ps.districts_upgrades  du
on d.district_id= du.district_id
and d.funding_year = du.funding_year

left join ps.districts_peers dp
on d.district_id= dp.district_id
and d.funding_year = dp.funding_year + 1

where d.funding_year = 2019
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and dfa2.fit_for_ia_cost = true
and d.state_code != 'AK'
order by cost_decrease_indicator