SELECT
  fit.funding_year,
  MEDIAN(bc.ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps_all,
  MEDIAN(bc.ia_bandwidth_per_student_kbps) FILTER (WHERE bc.meeting_2014_goal_no_oversub = true) as median_ia_bandwidth_per_student_kbps_meeting

FROM
  ps.districts_fit_for_analysis fit

  JOIN ps.districts dd
  ON fit.district_id = dd.district_id
  AND fit.funding_year = dd.funding_year

  JOIN ps.districts_bw_cost bc
  ON fit.district_id = bc.district_id
  AND fit.funding_year = bc.funding_year

WHERE
  fit.funding_year = 2019
  AND dd.district_type = 'Traditional'
  AND dd.in_universe = true
  AND fit.fit_for_ia = true

GROUP BY
  fit.funding_year

UNION

SELECT
  fit.funding_year,
  MEDIAN(bc.ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps_all,
  MEDIAN(bc.ia_bandwidth_per_student_kbps) FILTER (WHERE bc.meeting_2014_goal_no_oversub = true) as median_ia_bandwidth_per_student_kbps_meeting

FROM
  ps.districts_fit_for_analysis_frozen_sots fit

  JOIN ps.districts_frozen_sots dd
  ON fit.district_id = dd.district_id
  AND fit.funding_year = dd.funding_year

  JOIN ps.districts_bw_cost_frozen_sots bc
  ON fit.district_id = bc.district_id
  AND fit.funding_year = bc.funding_year

  WHERE
    dd.district_type = 'Traditional'
    AND fit.fit_for_ia = true

GROUP BY
  fit.funding_year


ORDER BY
  funding_year asc
