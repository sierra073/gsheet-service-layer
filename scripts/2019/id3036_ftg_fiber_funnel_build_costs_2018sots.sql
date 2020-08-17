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
),

--***FTG Query above***

 fiber_at_door as (
SELECT
SUM (CASE WHEN fds.distance_to_fiber <= 200
      THEN 1
ELSE 0 END)/COUNT(c.campus_id)::numeric as percent_fiber_at_door,
SUM (CASE WHEN fds.distance_to_fiber <= 750
        THEN 1
ELSE 0 END)/COUNT(c.campus_id)::numeric as percent_fiber_on_block

FROM ps.districts d

LEFT JOIN ps.districts_fiber df
ON d.district_id = df.district_id
AND d.funding_year = df.funding_year

LEFT JOIN ps.campuses c
ON d.district_id = c.district_id
AND d.funding_year = c.funding_year

LEFT JOIN ps.campuses_fiber cf
ON c.campus_id = cf.campus_id
AND c.funding_year = cf.funding_year

LEFT JOIN ps.campuses_fit_for_analysis cfa
ON c.campus_id = cfa.campus_id
AND c.funding_year = cfa.funding_year

LEFT JOIN ps.campuses_fit_for_analysis lycfa
ON c.campus_id = lycfa.campus_id
AND c.funding_year = lycfa.funding_year +1

JOIN ps.fiber_distance_static fds
ON c.campus_id =fds.campus_id
AND c.funding_year = fds.funding_year

LEFT JOIN ps.districts_sp_assignments sp
ON d.district_id = sp.district_id
AND d.funding_year = sp.funding_year

WHERE d.district_type = 'Traditional'
AND d.in_universe = true
AND d.funding_year = 2019
AND ( (cfa.campus_fit_for_campus = true
        AND cfa.category = 'Unscalable')
        OR
        ( cfa.campus_fit_for_campus = false
        AND lycfa.campus_fit_for_campus = true
        AND lycfa.category = 'Unscalable')
        )
AND df.fiber_target_status = 'Target'
AND (sp.primary_sp IN ('AT&T','CenturyLink','Comcast','Spectrum')
OR sp.primary_sp ILIKE '%Verizon%')
AND d.state_code Not IN ('AK','DC')
),

percents as (
  SELECT SUM(CASE WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
            THEN (ftg.median_total_cost)
            ELSE 0
      END)/SUM(ftg.median_total_cost) AS state_match_percent_w_exp,
      SUM(CASE WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
            AND d.c1_discount_rate >= .8
            THEN (ftg.median_total_cost)
            ELSE 0
      END)/SUM(CASE WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
                          THEN ftg.median_total_cost END) AS free_state_match_percent_w_exp,
      SUM(CASE WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','MA','MD','ME',
                          'MO','MT','NC','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
            THEN (ftg.median_total_cost)
            ELSE 0
      END)/SUM(ftg.median_total_cost) AS state_match_percent_no_exp,
      SUM(CASE WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','MA','MD','ME',
                          'MO','MT','NC','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
            AND d.c1_discount_rate >= .8
            THEN (ftg.median_total_cost)
            ELSE 0
      END)/SUM(ftg.median_total_cost) AS free_state_match_percent_no_exp
  FROM ps.districts d

  LEFT JOIN ps.districts_fiber df
  ON d.district_id = df.district_id
  AND d.funding_year = df.funding_year

  LEFT JOIN ftg
  ON d.district_id = ftg.district_id
  AND d.funding_year = 2019

  WHERE d.district_type = 'Traditional'
  AND d.in_universe = true
  AND d.funding_year = 2019
  AND df.fiber_target_status = 'Target'
  AND d.state_code Not IN ('AK','DC')
  ),

total_ftg_sum AS (
SELECT SUM(ftg.median_total_cost) as total_ftg
FROM ftg

LEFT JOIN ps.districts d
ON ftg.district_id = d.district_id
AND d.funding_year = 2019

WHERE d.district_type = 'Traditional'
AND d.in_universe = true
AND d.funding_year = 2019
AND d.state_code Not IN ('AK','DC')
),

non_fiber_scalable as (
SELECT SUM(
      CASE
      WHEN li.connect_category NOT IN ('Cable','Fixed Wireless')
        THEN dli.num_lines END)::numeric
      / SUM(dli.num_lines)::numeric
      AS percent_non_cable_wireless
from ps.districts_line_items dli

JOIN ps.line_items li
ON dli.line_item_id = li.line_item_id
AND dli.funding_year = li.funding_year

JOIN ps.districts d
ON dli.district_id = d.district_id
AND dli.funding_year = d.funding_year

JOIN ps.districts_fiber df
ON dli.district_id = df.district_id
AND dli.funding_year = df.funding_year

JOIN ps.districts_fit_for_analysis fit
ON dli.district_id = fit.district_id
AND dli.funding_year = fit.funding_year

WHERE d.in_universe = 'True'
AND d.district_type = 'Traditional'
AND df.fiber_target_status = 'Target'
AND li.purpose not in ('backbone','isp')
AND d.funding_year = 2019
AND li.connect_category NOT ILIKE '%Fiber%'
AND fit.fit_for_wan = true
AND d.state_code Not IN ('AK','DC')
)


 SELECT ROUND(tfs.total_ftg* fad.percent_fiber_on_block,-6) AS num_fiber_at_door,

      ROUND(tfs.total_ftg* (1-fad.percent_fiber_on_block),-6 )AS need_build,

      ROUND((tfs.total_ftg* (1-fad.percent_fiber_on_block)) --need build
      * state_match_percent_w_exp,-6) as state_match_w_exp,

      ROUND((tfs.total_ftg* (1-fad.percent_fiber_on_block)) --need build
      * (1- state_match_percent_w_exp),-6) as no_state_match_w_exp,


      ROUND((tfs.total_ftg* (1-fad.percent_fiber_on_block)) --need build
      * state_match_percent_w_exp * free_state_match_percent_w_exp,-6)AS free_state_match_w_exp,

      ROUND((tfs.total_ftg* (1-fad.percent_fiber_on_block)) --need build
      * state_match_percent_w_exp * (1-free_state_match_percent_w_exp),-6) AS paid_state_match_w_exp,

      ROUND((tfs.total_ftg* (1-fad.percent_fiber_on_block) --need build
      * (1- state_match_percent_w_exp)) * nfs.percent_non_cable_wireless,-6)AS no_state_match_unscalable_non_fiber,

      ROUND((tfs.total_ftg* (1-fad.percent_fiber_on_block) --need build
      * (1- state_match_percent_w_exp)) * (1 - nfs.percent_non_cable_wireless),-6) AS no_state_match_scalable_non_fiber


      /*ROUND((nu.unscalable_campuses* (1-fad.percent_fiber_on_block)) --need build
      * state_match_percent_no_exp) as state_match_no_exp,

     ROUND( (nu.unscalable_campuses* (1-fad.percent_fiber_on_block)) --need build
      *(1 - state_match_percent_no_exp)) as no_state_match_no_exp,

      ROUND((nu.unscalable_campuses* (1-fad.percent_fiber_on_block)) --need build
      * state_match_percent_no_exp * free_state_match_percent_w_exp )as free_state_match_no_exp,

      ROUND((nu.unscalable_campuses* (1-fad.percent_fiber_on_block)) --need build
      * state_match_percent_no_exp * (1-free_state_match_percent_no_exp) )AS paid_state_match_no_exp*/

FROM total_ftg_sum tfs,
fiber_at_door fad,
percents pc,
non_fiber_scalable nfs
