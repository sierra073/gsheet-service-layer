select
  d.district_id,
  d.state_code,
  dbc.ia_monthly_cost_per_mbps

from ps.districts d

join ps.districts_fit_for_analysis fit
on d.district_id = fit.district_id
and d.funding_year = fit.funding_year

join ps.districts_bw_cost dbc
on d.district_id = dbc.district_id
and d.funding_year = dbc.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and fit.fit_for_ia_cost = true
and d.funding_year = 2019
