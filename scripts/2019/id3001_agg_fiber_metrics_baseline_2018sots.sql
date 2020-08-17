WITH fy17_campuses as (

SELECT 'all'::varchar as population,
SUM(fib.assumed_unscalable_campuses + fib.known_unscalable_campuses) as unscalable_campuses_17
from ps.districts_fiber_frozen_sots fib

LEFT JOIN ps.districts_frozen_sots d
on fib.district_id = d.district_id
and fib.funding_year = d.funding_year

WHERE d.district_type = 'Traditional'
AND d.funding_year = 2018

),

current as (

SELECT 'all'::varchar as population,
      SUM(fib.assumed_unscalable_campuses_fine_wine + fib.known_unscalable_campuses_fine_wine) AS unscalable_campuses,
      /*SUM(lyfib.assumed_unscalable_campuses + lyfib.known_unscalable_campuses) AS unscalable_campuses_17,
      (SUM(lyfib.assumed_unscalable_campuses + lyfib.known_unscalable_campuses) -
          SUM(fib.assumed_unscalable_campuses + fib.known_unscalable_campuses))/
          SUM(lyfib.assumed_unscalable_campuses + lyfib.known_unscalable_campuses) AS percent_decrease_unscalable_campuses,*/
      (SUM(d.num_campuses) - SUM(fib.assumed_unscalable_campuses_fine_wine + fib.known_unscalable_campuses_fine_wine))/SUM(d.num_campuses) AS percent_scalable_campuses,
      SUM(fib.assumed_unscalable_campuses_fine_wine + fib.known_unscalable_campuses_fine_wine)/SUM(d.num_campuses) AS percent_unscalable_campuses,
      SUM(CASE WHEN d.locale IN ('Rural','Town') THEN (fib.assumed_unscalable_campuses_fine_wine + fib.known_unscalable_campuses_fine_wine) END)/
      SUM(fib.assumed_unscalable_campuses_fine_wine + fib.known_unscalable_campuses_fine_wine) as rural_percent_of_unscalable,
      COUNT(CASE WHEN fib.fiber_target_status = 'Target' then d.district_id END) AS fiber_targets
FROM ps.smd_2019_fine_wine  fib

JOIN ps.districts_fiber  dfib
ON fib.district_id = dfib.district_id
and fib.funding_year = dfib.funding_year

JOIN ps.districts d
ON fib.district_id = d.district_id
and fib.funding_year = d.funding_year


WHERE d.in_universe = true
AND d.district_type = 'Traditional'
AND d.funding_year = 2019

)

SELECT unscalable_campuses,
      unscalable_campuses_17,
      percent_scalable_campuses,
      percent_unscalable_campuses,
      rural_percent_of_unscalable,
      (unscalable_campuses_17 - unscalable_campuses)/unscalable_campuses_17 AS percent_decrease_unscalable_campuses

FROM current c

LEFT JOIN fy17_campuses ly
ON c.population = ly.population
