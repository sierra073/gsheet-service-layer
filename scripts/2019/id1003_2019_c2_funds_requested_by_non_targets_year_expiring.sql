--Of those requesting C2 in 2019 besides Targets, who were they?
--Specifically what year was/is their funding expiring?

WITH start_year AS (

  SELECT year_started,
    ROUND(SUM(budget_allocated * (c2_discount_rate * .01))) AS year_totals
  FROM
    dm.nassd_districts nd

  WHERE
    funding_year = 2019
    AND previous_year_wifi_status != 'Target'
    AND budget_allocated > 0

  GROUP BY year_started
),

total AS (
  SELECT
    ROUND(SUM(budget_allocated * (c2_discount_rate * .01))) AS total_c2_erate_requested,
    ROUND(SUM(budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE previous_year_wifi_status != 'Target')) AS non_target_c2_erate_requested

  FROM
    dm.nassd_districts

  WHERE
    funding_year = 2019
)

SELECT
  ss.year_started,
  CASE
    WHEN ss.year_started = 2015 THEN '2019'
    WHEN ss.year_started = 2016 THEN '2020'
    WHEN ss.year_started = 2017 THEN '2021'
    WHEN ss.year_started = 2018 THEN '2022'
    WHEN ss.year_started = 2019 THEN '2023'
    END AS year_c2_expiring,
  ss.year_totals,
  ROUND(ss.year_totals/tt.total_c2_erate_requested,2) AS percent_of_total,
  ROUND(ss.year_totals/tt.non_target_c2_erate_requested,2) AS percent_of_non_target_portion

FROM
  start_year ss

  CROSS JOIN
    total tt

GROUP BY ss.year_started, ss.year_totals, tt.total_c2_erate_requested, tt.non_target_c2_erate_requested
ORDER BY ss.year_started ASC
