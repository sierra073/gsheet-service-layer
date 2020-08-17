with campus_build_costs_corrected as (
select
  campus_id,
  district_id,
  case when c1_discount_rate is null then 0.7 else c1_discount_rate end as c1_discount_rate,
  median_total_cost_wan,
  case when c1_discount_rate is not null then median_total_erate_funding_wan
    else (0.8*median_total_cost_wan) end as median_total_erate_funding_wan,
  median_total_state_funding_wan,
  case when c1_discount_rate is not null then median_total_district_funding_wan
    else median_total_cost_wan - (0.8*median_total_cost_wan) - median_total_state_funding_wan
  end as median_total_district_funding_wan

  from dm.campus_build_costs
  where total_cost_median_wan > 0
),

district_build_costs_corrected as (
select
  district_id,
  case when c1_discount_rate is null then 0.7 else c1_discount_rate end as c1_discount_rate,
  total_cost_ia,
  case when c1_discount_rate is not null then total_erate_funding_ia
    else (0.8*total_cost_ia) end as total_erate_funding_ia,
  total_state_funding_ia,
  case when c1_discount_rate is not null then total_district_funding_ia
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
)

--***FTG Query above***

SELECT fds.campus_id,
      d.state_code,
      CASE
      WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
         'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        THEN True
        ELSE false
      END as state_match,
      ftg.median_total_cost,
      ftg.median_total_district_cost
FROM ps.fiber_distance_static fds

LEFT JOIN ftg
ON fds.campus_id = ftg.campus_id
AND fds.funding_year = 2019

JOIN ps.districts d
ON ftg.district_id = d.district_id
AND d.funding_year = 2019

LEFT JOIN ps.districts_sp_assignments sp
ON d.district_id = sp.district_id
AND d.funding_year = sp.funding_year

WHERE d.in_universe = true
AND d.district_type = 'Traditional'
AND d.funding_year = 2019
AND d.state_code Not IN ('AK','DC')
AND fds.distance_to_fiber > 750
/*AND (sp.primary_sp IN ('AT&T','CenturyLink','Comcast','Spectrum')
OR sp.primary_sp ILIKE '%Verizon%')
*/
