with numbers as(
SELECT
  dd.funding_year,
  SUM(dd.num_students) FILTER (WHERE fit.fit_for_ia = true AND bb.meeting_2018_goal_oversub = true) as num_students_meeting_sample,
  SUM(dd.num_students) as num_students_pop,
  SUM(dd.num_students) FILTER (WHERE fit.fit_for_ia = true) as num_students_clean

FROM
  ps.districts dd

  JOIN ps.districts_bw_cost bb
  ON dd.district_id = bb.district_id
  AND dd.funding_year = bb.funding_year

  JOIN ps.districts_fit_for_analysis fit
  ON dd.district_id = fit.district_id
  AND dd.funding_year = fit.funding_year

WHERE
  dd.in_universe = true
  AND dd.district_type = 'Traditional'


GROUP BY
  dd.funding_year

ORDER BY
  dd.funding_year
)

SELECT
  funding_year,
  round(num_students_meeting_sample::numeric/num_students_clean*num_students_pop/1000000,1) as num_students_meeting_ext

FROM
  numbers
