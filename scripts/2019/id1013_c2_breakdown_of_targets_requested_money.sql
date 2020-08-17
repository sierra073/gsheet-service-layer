with totals as (

  SELECT
    sum(budget_allocated * (c2_discount_rate * .01)) as total_allocated,
    sum(budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE year_started = 2019) as new_users,
    count(distinct district_id) FILTER (WHERE year_started = 2019) as num_new_users,
    sum(budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE year_started != 2019) as expiring_users,
    count(distinct district_id) FILTER (WHERE year_started != 2019) as num_expiring_users

  FROM
    dm.nassd_districts

  WHERE
    funding_year = 2019
    AND previous_year_wifi_status = 'Target'
    AND year_started is not NULL
    AND budget_allocated > 0
)

SELECT
  round(new_users/total_allocated,2) as perc_of_new_users,
  round(expiring_users/total_allocated,2) as perc_expiring_money,
  num_expiring_users,
  num_new_users

FROM
  totals
