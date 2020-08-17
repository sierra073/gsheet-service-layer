With pct_unscalable AS (

  SELECT df.funding_year,
        d.state_code,
        ROUND(SUM(df.assumed_unscalable_campuses + df.known_unscalable_campuses)/SUM(d.num_campuses),2) as percent_unscalable
  FROM ps.smd_2019_fine_wine  df

  JOIN ps.districts d
  ON df.district_id = d.district_id
  AND df.funding_year = d.funding_year

  WHERE d.in_universe = true
  AND d.district_type = 'Traditional'
  AND d.state_code != 'DC'


  GROUP BY 1,2




)

SELECT
      funding_year,
      SUM(CASE WHEN percent_unscalable <= .01 THEN 1 ELSE 0 END) as num_states_99_pct_fiber
FROM pct_unscalable

GROUP BY funding_year

ORDER BY funding_year
