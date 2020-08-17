select SUM(CASE
            WHEN d.funding_year = 2018
            AND dff.fiber_target_status = 'Target'
              THEN 1 ELSE 0 END)
      - SUM(CASE
                  WHEN d.funding_year = 2019
                  AND df.fiber_target_status = 'Target'
                    THEN 1 ELSE 0 END)
      AS upgraded_to_fiber
FROM ps.districts d

JOIN ps.districts_fiber df
ON  d.district_id = df.district_id
AND d.funding_year = df.funding_year

LEFT JOIN ps.districts_fiber_frozen_sots dff
ON  df.district_id = dff.district_id
AND df.funding_year = dff.funding_year

WHERE d.in_universe = True
AND d.district_type = 'Traditional'
