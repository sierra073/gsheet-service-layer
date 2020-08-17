WITH district_lkp AS (

  SELECT
    DISTINCT dw.district_id

    FROM
      ps.districts_wifi dw

      JOIN ps.districts d
      ON d.district_id = dw.district_id
      AND d.funding_year = dw.funding_year


    WHERE
      d.district_type = 'Traditional'
      AND d.in_universe = true
      AND dw.remaining/dw.budget > .1
      AND dw.remaining/dw.budget < .5
      AND dw.funding_year = 2018
      AND dw.year_started = 2015
  ),

by_year AS (
  SELECT
    dw.funding_year,
    sum(dw.budget_post - dw.remaining_post) AS c2_used_post,
    sum(dw.budget - dw.remaining) AS c2_used_pre

  FROM
    district_lkp dl

    JOIN ps.districts_wifi dw
    ON dl.district_id = dw.district_id

  GROUP BY
    dw.funding_year
),

budget_only as (
  SELECT
    bi15.c2_used_post AS used_post_2015,
    bi16.c2_used_post - bi15.c2_used_post AS used_post_2016,
    bi17.c2_used_post - bi16.c2_used_post AS used_post_2017,
    bi18.c2_used_post - bi17.c2_used_post AS used_post_2018,
    bi19.c2_used_post - bi18.c2_used_post AS used_post_2019,
    bi15.c2_used_pre AS used_pre_2015,
    bi16.c2_used_pre - bi15.c2_used_pre AS used_pre_2016,
    bi17.c2_used_pre - bi16.c2_used_pre AS used_pre_2017,
    bi18.c2_used_pre - bi17.c2_used_pre AS used_pre_2018,
    bi19.c2_used_pre - bi18.c2_used_pre AS used_pre_2019


    FROM
    by_year bi15

    CROSS JOIN by_year bi16

    CROSS JOIN by_year bi17

    CROSS JOIN by_year bi18

    CROSS JOIN by_year bi19

    WHERE
    bi15.funding_year = 2015
    AND bi16.funding_year = 2016
    AND bi17.funding_year = 2017
    AND bi18.funding_year = 2018
    AND bi19.funding_year = 2019
),

expiring_not_targets AS (
  SELECT
    ww.district_id

    FROM
      dm.nassd_districts ww

  WHERE
    ww.year_started = 2015
    AND ww.funding_year = 2018
    AND ww.percent_c2_budget_remaining > .1
    AND ww.percent_c2_budget_remaining < .5
)

SELECT
  COUNT(ee.district_id) as num_districts_exp_btw_50_ad_10_pct,
  COUNT(ee.district_id) FILTER (WHERE ww.budget_allocated > 0) AS districts_that_requested_c2,
  ROUND(SUM(ww.budget_allocated * (ww.c2_discount_rate * .01)),2) as dollars_requested,
  bb.used_post_2019 as dollars_allowed_by_budget

FROM
  expiring_not_targets ee

  JOIN dm.nassd_districts ww
  ON ee.district_id = ww.district_id
  AND ww.funding_year = 2019

  CROSS JOIN budget_only bb

GROUP BY
  bb.used_post_2019
