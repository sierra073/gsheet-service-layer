with totals as (

  SELECT
    sum(budget_allocated * (c2_discount_rate * .01)) as total_allocated,
    sum(budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE year_started = 2019) as new_users,
    sum(budget_allocated * (c2_discount_rate * .01)) FILTER (WHERE year_started != 2019) as expiring_users

  FROM
    dm.nassd_districts

  WHERE
    funding_year = 2019
    AND previous_year_wifi_status = 'Target'
)

SELECT
  round(new_users/total_allocated,2) as perc_of_new_users,
  round(expiring_users/total_allocated,2) as perc_expiring_money

FROM
  totals
