with master as (
SELECT
  dd.funding_year,
  sum(dd.num_students) FILTER (WHERE bw.meeting_2018_goal_oversub = true AND fit.fit_for_ia = true) as num_students_meeting_sample,
  sum(dd.num_students) as student_population,
  sum(dd.num_students) FILTER (WHERE fit.fit_for_ia = true) as clean_population

FROM
  ps.districts dd

  JOIN ps.districts_bw_cost bw
  ON dd.district_id = bw.district_id
  AND dd.funding_year = bw.funding_year

  JOIN ps.districts_fit_for_analysis fit
  ON dd.district_id = fit.district_id
  AND dd.funding_year = fit.funding_year

WHERE
  dd.in_universe = true
  AND dd.funding_year = 2019
  AND dd.district_type = 'Traditional'

GROUP BY
  dd.funding_year

UNION

SELECT
  dd.funding_year,
  sum(dd.num_students) FILTER (WHERE bw.meeting_2018_goal_oversub = true AND fit.fit_for_ia = true) as num_students_meeting_sample,
  sum(dd.num_students) as student_population,
  sum(dd.num_students) FILTER (WHERE fit.fit_for_ia = true) as clean_population

FROM
  ps.districts dd

  JOIN ps.districts_bw_cost_frozen_sots bw
  ON dd.district_id = bw.district_id
  AND dd.funding_year = bw.funding_year

  JOIN ps.districts_fit_for_analysis_frozen_sots fit
  ON dd.district_id = fit.district_id
  AND dd.funding_year = fit.funding_year

WHERE
  dd.in_universe = true
  AND dd.funding_year != 2019
  AND dd.district_type = 'Traditional'

GROUP BY
  dd.funding_year

ORDER BY
  funding_year
)

SELECT
  funding_year,
  round(num_students_meeting_sample::numeric/clean_population*student_population/1000000,1) as num_students

FROM
  master

GROUP BY
  funding_year,
  num_students_meeting_sample,
  clean_population,
  student_population

ORDER BY
  funding_year
