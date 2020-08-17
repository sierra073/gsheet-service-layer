with start as (
SELECT
  dd.locale,
  CASE
    WHEN dd.locale in ('Rural','Town')
      THEN 'Rural/Town'
    WHEN dd.locale in ('Suburban','Urban')
      THEN 'Urban/Suburban'
    END as new_locale,
  COUNT(dd.district_id) as district_pop,
  COUNT(dd.district_id) FILTER (WHERE fit.fit_for_ia = TRUE) as district_pop_clean,
  COUNT(dd.district_id) FILTER (WHERE fit.fit_for_ia = TRUE AND bw.meeting_2018_goal_oversub = TRUE) as district_meeting_sample

FROM
  ps.districts dd

  JOIN ps.districts_bw_cost bw
  ON dd.district_id = bw.district_id
  AND dd.funding_year = bw.funding_year

  JOIN ps.districts_fit_for_analysis fit
  ON dd.district_id = fit.district_id
  AND dd.funding_year = fit.funding_year

WHERE
  dd.funding_year = 2019
  AND dd.in_universe = TRUE
  AND dd.district_type = 'Traditional'

GROUP BY
  dd.locale
),

totals as (
  SELECT
    new_locale,
    SUM(district_pop) as district_pop,
    SUM(district_pop_clean) as district_pop_clean,
    SUM(district_meeting_sample) as district_meeting_sample

  FROM start

  GROUP BY
    new_locale
),

extrap as (
  SELECT
    new_locale,
    district_pop,
    round(district_meeting_sample::numeric/district_pop_clean*district_pop) as num_districts_meeting_ext,
    12910 as total_pop

  FROM
    totals

  UNION

  SELECT
    'total' as new_locale,
    sum(district_pop),
    round(sum(district_meeting_sample::numeric/district_pop_clean*district_pop)) as num_districts_meeting_ext,
    sum(district_pop) as total_pop

  FROM
    totals

)

SELECT
  new_locale,
  round(num_districts_meeting_ext/total_pop,2) as percent_of_whole

FROM
  extrap
