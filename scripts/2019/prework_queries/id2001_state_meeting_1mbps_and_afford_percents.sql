select
  d.state_code,
  d.funding_year,
  (count(distinct d.district_id) filter(where dbc.ia_monthly_cost_per_mbps <= 3.0))::float/count(distinct d.district_id)::float as perc_meeting_affordability,
  (count(distinct d.district_id) filter(where dbc.meeting_2018_goal_oversub = true))::float/count(distinct d.district_id)::float as perc_meeting_1mbps

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
and state_code != 'DC'

group by 1,2

order by 3 desc
