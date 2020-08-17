with population as (
  SELECT
    dd.district_id,
    dd.name,
    ww.year_started

  FROM
   ps.districts dd

    LEFT JOIN ps.districts_wifi ww
    ON ww.district_id = dd.district_id
    AND ww.funding_year = dd.funding_year

  WHERE
   ww.year_started = dd.funding_year
   AND ww.year_started IS NOT NULL
   AND dd.in_universe = true
   AND dd.district_type = 'Traditional'
),

totals as (
  SELECT
   nn.funding_year,
   count(distinct nn.district_id) FILTER (WHERE nn.percent_c2_budget_remaining = 1) as num_districts_non_starters

  FROM
    dm.nassd_districts nn

   JOIN ps.districts dd
    ON dd.district_id = nn.district_id
    AND dd.funding_year = nn.funding_year

  WHERE
    dd.in_universe = true
    and dd.district_type = 'Traditional'

  GROUP BY
    nn.funding_year

UNION

  SELECT
    2014 as funding_year,
    count(distinct nn.district_id) FILTER (WHERE nn.funding_year = 2015) as all_2015

  FROM
    dm.nassd_districts nn

    JOIN ps.districts dd
    ON dd.district_id = nn.district_id
    AND dd.funding_year = nn.funding_year

  WHERE
    dd.in_universe = true
    AND dd.district_type = 'Traditional'
)

SELECT
pp.year_started,
round(count(pp.district_id)::integer/num_districts_non_starters::numeric,2) as perc_starting,
count(pp.district_id) as num_districts_started,
num_districts_non_starters as districts_not_started,
sum(ww.budget_used) as budget_requested

FROM
  population pp

  JOIN ps.districts_wifi ww
  ON ww.funding_year = pp.year_started
  AND ww.district_id = pp.district_id

  JOIN totals tt
  ON (tt.funding_year + 1) = pp.year_started

GROUP BY
  pp.year_started, num_districts_non_starters

ORDER BY
  pp.year_started asc
