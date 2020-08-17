WITH district_lkp AS (

  SELECT
    DISTINCT dw.district_id

    FROM
      ps.districts_wifi dw

      JOIN ps.districts d
      ON d.district_id = dw.district_id
      AND d.funding_year = dw.funding_year

    WHERE
      dw.c2_received = true
      AND d.district_type = 'Traditional'
      AND d.in_universe = true
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
)

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
  and bi17.funding_year = 2017
  and bi18.funding_year = 2018
  and bi19.funding_year = 2019
