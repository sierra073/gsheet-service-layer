SELECT
  d.district_id,
  d.funding_year,
  d.in_universe,
  d.district_type,
  d.state_code,
  d.locale,
  d.size,
  d.num_students,
  dffa.fit_for_ia,
  dffa.fit_for_ia_cost,
  dbw.ia_bw_mbps_total,
  du.upgrade_indicator,
  dbw.meeting_2014_goal_no_oversub,
  dbw.meeting_2018_goal_oversub
FROM
  ps.districts d
  JOIN ps.districts_bw_cost dbw
  ON d.district_id = dbw.district_id
  AND d.funding_year = dbw.funding_year
  JOIN ps.districts_fit_for_analysis dffa
  ON d.district_id = dffa.district_id
  AND d.funding_year = dffa.funding_year
  JOIN ps.districts_upgrades du
  ON d.district_id = du.district_id
  AND d.funding_year = du.funding_year
