--How much money have the targets been spending year over year (2018 wifi targets)

SELECT
  nd.funding_year,
  ROUND(SUM(nd.budget_allocated * (nd.c2_discount_rate * .01))) AS total_erate_requested

FROM
  dm.nassd_districts nd

  JOIN dm.nassd_districts nd18
  ON nd.district_id = nd18.district_id
  AND nd18.wifi_target_status = 'Target'
  AND nd18.funding_year = 2018

GROUP BY
  nd.funding_year

ORDER BY
  nd.funding_year ASC
