with temp as (select 
  d.district_id,
  
  case when dbc2.ia_monthly_cost_per_mbps > dbc.ia_monthly_cost_per_mbps
    then true
  else false end as cost_decrease_indicator,
 
  case when dl.most_recent_ia_contract_end_date > dl2.most_recent_ia_contract_end_date
    then true
  else false end as changed_contract_indicator,
  
  du.switcher as switched_sp

from ps.districts d

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

where d.funding_year = 2019
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and dfa2.fit_for_ia_cost = true
and d.state_code != 'AK')

select count(case when switched_sp then district_id end)::numeric/count(*)
as pct_switched_sp
from temp
where cost_decrease_indicator = true
and changed_contract_indicator = true