with state_wtavg_cost_mbps as (
    select
      d.state_code,
      sum(dbc.ia_monthly_cost_total)/sum(dbc.ia_bw_mbps_total) as wtavg_cost_per_mbps,
      CASE
        WHEN sum(dbc.ia_monthly_cost_total)/sum(dbc.ia_bw_mbps_total) > 3
          then 'States Not Meeting $3/Mbps'
        ELSE 'States Meeting $3/Mbps'
      END as state_status

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

    group by 1),

    districts_1mbps_status as (
    select
      d.state_code,
      d.district_id,
      d.funding_year,
      dbc.meeting_2018_goal_oversub

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
    and d.funding_year = 2019)

select
state_status,
(count(distinct district_id) filter(where meeting_2018_goal_oversub = true))::float/count(distinct district_id)::float as districts_perc_meeting_1mbps

from state_wtavg_cost_mbps swc

join districts_1mbps_status d1s
on swc.state_code = d1s.state_code

group by 1
