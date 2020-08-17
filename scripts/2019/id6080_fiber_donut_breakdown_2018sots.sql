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

),

states as (
  SELECT
        state_code,
        SUM(CASE 
              WHEN funding_year = 2015
                then 1-percent_unscalable
              ELSE 0 
            END) as pct_fiber_2015,
        SUM(CASE 
              WHEN funding_year = 2017
                then 1-percent_unscalable
              ELSE 0 
            END) as pct_fiber_2017,
        SUM(CASE 
              WHEN funding_year = 2018
                then 1-percent_unscalable
              ELSE 0 
            END) as pct_fiber_2018,
        SUM(CASE 
              WHEN funding_year = 2019
                then 1-percent_unscalable
              ELSE 0 
            END) as pct_fiber_2019
  FROM pct_unscalable
  
  GROUP BY state_code
)

select *
from states
where pct_fiber_2019 >= .99
or pct_fiber_2018 >= .99
or pct_fiber_2017 >= .99
or pct_fiber_2015 >= .99
order by 
  pct_fiber_2015 >= .99 desc,
  pct_fiber_2017 >= .99 desc,
  pct_fiber_2018 >= .99 desc,
  pct_fiber_2019 >= .99 desc,
  pct_fiber_2019 desc,
  pct_fiber_2018 desc,
  pct_fiber_2017 desc,
  pct_fiber_2015 desc,
  state_code
