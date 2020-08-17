select 'median' as metric,
  case when meeting_2018_goal_oversub = true then 'Meeting 1 Mbps'
  else 'Not Meeting 1 Mbps: Pay More' end as meeting_2018_goal_oversub,
  median(dbc.ia_monthly_cost_per_mbps) as ia_monthly_cost_per_mbps

from ps.districts d

join ps.districts_fit_for_analysis fit
on d.district_id = fit.district_id
and d.funding_year = fit.funding_year

join ps.districts_bw_cost dbc
on d.district_id = dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_upgrades du
on d.district_id = du.district_id
and d.funding_year = du.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and fit.fit_for_ia_cost = true
and d.funding_year = 2019
and (meeting_2018_goal_oversub = true or path_to_meet_2018_goal_group = 'Pay More')
group by 1,2

UNION

select 'weighted avg' as metric,
  case when meeting_2018_goal_oversub = true then 'Meeting 1 Mbps'
  else 'Not Meeting 1 Mbps: Pay More' end as meeting_2018_goal_oversub,
  sum(dbc.ia_monthly_cost_total)::numeric/sum(ia_bw_mbps_total) as ia_monthly_cost_per_mbps

from ps.districts d

join ps.districts_fit_for_analysis fit
on d.district_id = fit.district_id
and d.funding_year = fit.funding_year

join ps.districts_bw_cost dbc
on d.district_id = dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_upgrades du
on d.district_id = du.district_id
and d.funding_year = du.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and fit.fit_for_ia_cost = true
and d.funding_year = 2019
and (meeting_2018_goal_oversub = true or path_to_meet_2018_goal_group = 'Pay More')
group by 1,2
