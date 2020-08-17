with campus_build_costs_corrected as (
select
  campus_id,
  district_id,
  case when (c1_discount_rate is null OR c1_discount_rate = 0) then 0.7 else c1_discount_rate end as c1_discount_rate,
  median_total_cost_wan,
  case when c1_discount_rate is not null AND c1_discount_rate != 0 then median_total_erate_funding_wan
    else (0.8*median_total_cost_wan) end as median_total_erate_funding_wan,
  median_total_state_funding_wan,
  case when c1_discount_rate is not null AND c1_discount_rate != 0 then median_total_district_funding_wan
    else median_total_cost_wan - (0.8*median_total_cost_wan) - median_total_state_funding_wan
  end as median_total_district_funding_wan

  from dm.campus_build_costs
  where total_cost_median_wan > 0
),

district_build_costs_corrected as (
select
  district_id,
  case when (c1_discount_rate is null OR c1_discount_rate = 0) then 0.7 else c1_discount_rate end as c1_discount_rate,
  total_cost_ia,
  case when c1_discount_rate is not null AND c1_discount_rate != 0 then total_erate_funding_ia
    else (0.8*total_cost_ia) end as total_erate_funding_ia,
  total_state_funding_ia,
  case when c1_discount_rate is not null AND c1_discount_rate != 0 then total_district_funding_ia
    else total_cost_ia - (0.8*total_cost_ia) - total_state_funding_ia
  end as total_district_funding_ia

  from dm.district_build_costs
  where total_cost_ia > 0
),

districts_num_campus_builds as (
  select
    district_id,
    count(distinct campus_id) as num_campus_builds

  from campus_build_costs_corrected
group by 1
),

district_build_costs as (
  select
    dbc.district_id,
    c1_discount_rate,
    total_cost_ia::numeric/(case when num_campus_builds > 0 then num_campus_builds else 1 end) as total_cost_ia_distributed,
    total_erate_funding_ia::numeric/(case when num_campus_builds > 0 then num_campus_builds else 1 end) as total_erate_funding_ia_distributed,
    total_state_funding_ia::numeric/(case when num_campus_builds > 0 then num_campus_builds else 1 end) as total_state_funding_ia_distributed,
    total_district_funding_ia::numeric/(case when num_campus_builds > 0 then num_campus_builds else 1 end) as total_district_cost_ia_distributed

  from district_build_costs_corrected dbc
  left join districts_num_campus_builds dnc
    on dbc.district_id = dnc.district_id


),

campus_build_costs as (
  select
    cbc.campus_id,
    cbc.district_id,
    cbc.c1_discount_rate,
    median_total_cost_wan + (case when total_cost_ia_distributed > 0 then total_cost_ia_distributed else 0 end) as median_total_cost,
    median_total_erate_funding_wan + (case when total_erate_funding_ia_distributed > 0 then total_erate_funding_ia_distributed else 0 end) as median_total_erate_funding,
    median_total_state_funding_wan + (case when total_state_funding_ia_distributed > 0 then total_state_funding_ia_distributed else 0 end) as median_total_state_funding,
    median_total_district_funding_wan + (case when total_district_cost_ia_distributed > 0 then total_district_cost_ia_distributed else 0 end) as median_total_district_cost

    from
    campus_build_costs_corrected cbc
    left join district_build_costs dbc
      on cbc.district_id = dbc.district_id
),

ftg AS (
select * from campus_build_costs order by district_id
),

--***FTG Query above***

/*the table metric is designed to streamline creating the fiber funnel for multiple different metrics
it is filtered to the fiber distance subset*/
dist_cost as (
SELECT fds.campus_id,
      case
      WHEN d.state_code in ('AZ','IL','MO','WA')
        THEN 0
      WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                        'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        THEN ftg.median_total_district_cost
      WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                        'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        THEN ftg.median_total_cost * .1
        END AS median_total_district_cost

FROM ps.fiber_distance_static fds

LEFT JOIN ps.campuses c
ON fds.campus_id = c.campus_id
AND fds.funding_year = c.funding_year

LEFT JOIN ps.districts d
ON c.district_id = d.district_id
AND c.funding_year = d.funding_year

LEFT JOIN ftg
ON fds.campus_id = ftg.campus_id

LEFT JOIN ps.districts_sp_assignments sp
ON d.district_id = sp.district_id
AND d.funding_year = sp.funding_year

LEFT JOIN ps.districts_fiber df
ON d.district_id = df.district_id
AND d.funding_year = df.funding_year

WHERE d.district_type = 'Traditional'
AND d.in_universe = true
AND d.funding_year = 2019
)
/*AND d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                  'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')*/


SELECT
 -- d.c1_discount_rate,
  COUNT(*) AS campuses,
  ROUND(AVG(dc.median_total_district_cost),2) AS avg_dist_cost
  FROM ps.fiber_distance_static fds

  LEFT JOIN ps.campuses c
  ON fds.campus_id = c.campus_id
  AND fds.funding_year = c.funding_year

  LEFT JOIN ps.districts d
  ON c.district_id = d.district_id
  AND c.funding_year = d.funding_year

  LEFT JOIN dist_cost dc
  ON fds.campus_id = dc.campus_id

  LEFT JOIN ps.districts_sp_assignments sp
  ON d.district_id = sp.district_id
  AND d.funding_year = sp.funding_year

  LEFT JOIN ps.districts_fiber df
  ON d.district_id = df.district_id
  AND d.funding_year = df.funding_year

  WHERE d.district_type = 'Traditional'
  AND d.in_universe = true
  AND d.funding_year = 2019
  AND df.fiber_target_status = 'Target'
  AND d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                    'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
  AND (fds.distance_to_fiber > 750
  OR fds.distance_to_fiber IS NULL )
  AND dc.median_total_district_cost > 0
  AND d.state_code NOT IN  ('AK','DC')
  AND (sp.primary_sp NOT IN ('AT&T','CenturyLink','Comcast','Spectrum')
        AND sp.primary_sp NOT ILIKE '%Verizon%')

--GROUP BY d.c1_discount_rate
