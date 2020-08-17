

WITH metrics as (
SELECT fw.district_id,
      CASE WHEN fw.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
            THEN true
            ELSE false
      END AS state_match,
      CASE WHEN d.ulocal::int IN (33,43) THEN true ELSE FALSE
        END AS super_remote_locale,
      d.c1_discount_rate,
      CASE WHEN fw.fit_for_ia = true THEN bw.meeting_2014_goal_no_oversub
      ELSE FALSE END AS meeting_2014_goal_no_oversub,
      CASE WHEN fw.fit_for_ia = true THEN bw.meeting_2018_goal_oversub
      ELSE FALSE END AS meeting_2018_goal_oversub,
      fib.fiber_target_status
FROM ps.smd_2019_fine_wine fw

LEFT JOIN ps.districts d
ON fw.district_id = d.district_id
ANd fw.funding_year = d.funding_year

LEFT JOIN ps.districts_fiber fib
ON fw.district_id = fib.district_id
AND fw.funding_year = fib.funding_year

LEFT JOIN ps.districts_bw_cost bw
ON fw.district_id = bw.district_id
AND fw.funding_year = bw.funding_year

LEFT JOIN ps.districts_sp_assignments sp
ON fw.district_id = sp.district_id
AND fw.funding_year = sp.funding_year

LEFT JOIN ps.districts_fit_for_analysis fit
ON fw.district_id = fit.district_id
AND fw.funding_year = fit.funding_year

LEFT JOIN ps.smd_2019_fine_wine lydf
ON fw.district_id = lydf.district_id
AND fw.funding_year = lydf.funding_year + 1

LEFT JOIN ps.districts_470s ffs
ON fw.district_id = ffs.district_id
AND fw.funding_year = ffs.funding_year


LEFT JOIN ps.districts_lines dli
ON fw.district_id = dli.district_id
AND fw.funding_year = dli.funding_year

WHERE fw.district_type = 'Traditional'
AND fw.funding_year = 2019
AND fw.fiber_target_status = 'Target'
),

t as (


SELECT district_id,
        CASE WHEN state_match  = true then '1 State Match'
          WHEN c1_discount_rate < .5 then '2 Wealthy District'
            WHEN super_remote_locale = true then '3 Super Remote'
            WHEN meeting_2018_goal_oversub THEN '4 Meeting 1 Mbps'
            WHEN meeting_2014_goal_no_oversub THEN '5 Meeting 100 kbps'
            ELSE '6 No Solution'
            END as fiber_funnel
FROM metrics m
)

SELECT fiber_funnel,
      COUNT(district_id)
FROM t

GROUP BY fiber_funnel

ORDER BY fiber_funnel
