--How much money was used by districts that wasn't expiring or less than 50% in 2019

SELECT
  ROUND(SUM(budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE previous_year_wifi_status = 'Target')) AS total_erate_requested_by_targets,
  ROUND(SUM(budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE previous_year_wifi_status != 'Target')) AS total_erate_requested_non_targets,
  ROUND(SUM(budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE year_started IN (2015,2019) AND previous_year_wifi_status != 'Target')) AS total_erate_requested_expiring,
  ROUND(SUM(budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE year_started IN (2016,2017,2018) AND previous_year_wifi_status != 'Target')) AS total_erate_requested_not_expiring
FROM
  dm.nassd_districts

WHERE
  funding_year = 2019
