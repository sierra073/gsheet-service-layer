with temp_t as (
select
  d.district_id,

  case when dbc2.ia_monthly_cost_per_mbps > 0
  then (dbc.ia_monthly_cost_per_mbps - dbc2.ia_monthly_cost_per_mbps)/dbc2.ia_monthly_cost_per_mbps::numeric
  end as pchg_cost_mbps,

  case when dbc2.ia_monthly_cost_total > 0
  then (dbc.ia_monthly_cost_total - dbc2.ia_monthly_cost_total)/dbc2.ia_monthly_cost_total::numeric
  end as pchg_cost_total,

  case when dbc2.ia_monthly_cost_per_mbps > dbc.ia_monthly_cost_per_mbps
    then true
  else false end as cost_decrease_indicator,

  du.added_fiber

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

join ps.districts_upgrades  du
on d.district_id= du.district_id
and d.funding_year = du.funding_year

where d.funding_year = 2018
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and dfa2.fit_for_ia_cost = true
and d.state_code != 'AK')

select median(pchg_cost_mbps)
from temp_t
where added_fiber = true
