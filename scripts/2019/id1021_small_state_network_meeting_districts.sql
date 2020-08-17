with student_totals as (
SELECT
  ss.state_network,
  dd.size,
  CASE
    WHEN dd.size in ('Small','Tiny')
      THEN 'Small/Tiny'
    WHEN dd.size in ('Large','Medium')
      THEN 'Large/Medium'
    WHEN dd.size = 'Mega' THEN 'Mega' END as new_size,
  COUNT(dd.district_id) FILTER (WHERE fit.fit_for_ia = true AND bb.meeting_2018_goal_oversub = true) as num_districts_meeting_sample,
  COUNT(dd.district_id) FILTER (WHERE fit.fit_for_ia = true AND bb.meeting_2018_goal_oversub = false) as num_districts_not_meeting_sample,
  COUNT(dd.district_id) as num_districts_pop,
  COUNT(dd.district_id) FILTER (WHERE fit.fit_for_ia = true) as num_districts_clean

FROM
  ps.districts dd

  JOIN ps.districts_bw_cost bb
  ON dd.district_id = bb.district_id
  AND dd.funding_year = bb.funding_year

  JOIN ps.districts_fit_for_analysis fit
  ON dd.district_id = fit.district_id
  AND dd.funding_year = fit.funding_year

  JOIN ps.states_static ss
  ON dd.state_code = ss.state_code

WHERE
  dd.in_universe = true
  AND dd.funding_year = 2019
  AND dd.district_type = 'Traditional'

GROUP BY
  ss.state_network,
  dd.size

ORDER BY
  ss.state_network asc
),

totals as (

SELECT
  state_network,
  new_size,
  sum(num_districts_pop) as num_districts_pop,
  sum(num_districts_clean) as num_districts_clean,
  sum(num_districts_meeting_sample) as num_districts_meeting_sample,
  sum(num_districts_not_meeting_sample) as num_districts_not_meeting_sample

FROM
  student_totals

GROUP BY
  state_network,
  new_size
  order by state_network asc
),

calc as (

SELECT
  state_network,
  new_size,
  num_districts_pop,
  round(num_districts_meeting_sample::numeric/num_districts_clean*num_districts_pop) as num_districts_meeting_ext,
  round(num_districts_not_meeting_sample::numeric/num_districts_clean*num_districts_pop) as num_districts_not_meeting_ext

FROM
  totals
)

SELECT
  state_network,
  new_size,
  round(num_districts_meeting_ext/num_districts_pop,2) as percent_meeting

FROM
  calc

WHERE
  new_size = 'Small/Tiny'
