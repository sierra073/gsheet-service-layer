with metrics as(
SELECT
  COUNT(distinct nd.district_id) as total_targets,
  COUNT(distinct nd.district_id) FILTER (WHERE year_started = 2019 OR year_started is NULL) as starter_targets,
  COUNT(distinct nd.district_id) FILTER (WHERE year_started in (2015,2016,2017,2018)) as expiring_targets,
  COUNT(distinct nd.district_id) FILTER (WHERE previous_year_c2_survey_completed = True) as reached_all,
  COUNT(distinct nd.district_id) FILTER (WHERE previous_year_c2_survey_completed = True AND (year_started = 2019 or year_started is NULL)) as reached_starters,
  COUNT(distinct nd.district_id) FILTER (WHERE previous_year_c2_survey_completed = True AND year_started in (2015,2016,2017,2018)) as reached_expiring,
  COUNT(distinct nd.district_id) FILTER (WHERE budget_allocated > 0 AND year_started = 2019 AND previous_year_c2_survey_completed = True) as reached_and_requested_starters,
  COUNT(distinct nd.district_id) FILTER (WHERE budget_allocated > 0 AND year_started != 2019 AND previous_year_c2_survey_completed = True) as reached_and_requested_expiring,
  COUNT(distinct nd.district_id) FILTER (WHERE budget_allocated > 0 AND year_started = 2019 AND previous_year_c2_survey_completed = false) as not_reached_request_starters,
  COUNT(distinct nd.district_id) FILTER (WHERE budget_allocated > 0 AND year_started != 2019 AND previous_year_c2_survey_completed = false) as not_reached_request_expiring,
  ROUND(SUM(nd.budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE budget_allocated > 0 AND previous_year_c2_survey_completed = True),2) as reached_erate_requested,
  ROUND(SUM(nd.budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE budget_allocated > 0 AND previous_year_c2_survey_completed = True AND year_started = 2019),2) as reached_erate_requested_starter,
  ROUND(SUM(nd.budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE budget_allocated > 0 AND previous_year_c2_survey_completed = True AND year_started != 2019),2) as reached_erate_requested_expiring,
  ROUND(SUM(nd.budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE budget_allocated > 0 AND previous_year_c2_survey_completed = False),2) as not_reached_erate_requested,
  ROUND(SUM(nd.budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE budget_allocated > 0 AND previous_year_c2_survey_completed = False AND year_started = 2019),2) as not_reached_erate_requested_starter,
  ROUND(SUM(nd.budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE budget_allocated > 0 AND previous_year_c2_survey_completed = False AND year_started != 2019),2) as not_reached_erate_requested_expiring

FROM
  dm.nassd_districts nd

WHERE
  funding_year = 2019
  AND previous_year_wifi_status = 'Target'
)

SELECT
  (reached_and_requested_starters + reached_and_requested_expiring)::numeric / reached_all as perc_reached_requested,
  reached_and_requested_starters::numeric / reached_starters as perc_starters_reached_requested,
  reached_and_requested_expiring::numeric / reached_expiring as perc_expiring_reached_requested,
  (not_reached_request_starters + not_reached_request_expiring)::numeric / (total_targets - reached_all) as perc_not_reached_requested,
  not_reached_request_starters::numeric / (starter_targets - reached_starters) as perc_starters_not_reached_requested,
  not_reached_request_expiring::numeric / (expiring_targets - reached_expiring) as perc_expiring_not_reached_requested,
  reached_erate_requested,
  reached_erate_requested_starter,
  reached_erate_requested_expiring,
  not_reached_erate_requested,
  not_reached_erate_requested_starter,
  not_reached_erate_requested_expiring

FROM
  metrics
