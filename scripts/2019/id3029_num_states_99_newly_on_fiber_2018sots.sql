With pct_unscalable AS (
SELECT df.funding_year,
      d.state_code,
      SUM(df.assumed_unscalable_campuses + df.known_unscalable_campuses)/SUM(d.num_campuses) as percent_unscalable
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
      COUNT(CASE WHEN percent_unscalable <= .015 AND funding_year = 2019 THEN 1  else NULL END)
      - COUNT(CASE WHEN percent_unscalable <= .015 AND funding_year = 2015 THEN 1  else NULL END)
      AS states_at_99
FROM pct_unscalable
