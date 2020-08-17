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

state_unscalable_table as (
SELECT
  d.state_code,
  COUNT(*) as unscalable_w_data,
  SUM(CASE WHEN fds.distance_to_fiber <= 750 THEN 1 ELSE 0 END)
    /COUNT(CASE WHEN d.state_code Not IN ('AK','DC') THEN 1 END)::numeric
    AS fiber_at_door_pct,

  CASE WHEN SUM(CASE WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
  'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
  AND fds.distance_to_fiber > 750
  AND d.c1_discount_rate IS NOT NULL
    THEN 1 ELSE 0 END) = 0 THEN 0 ELSE --remove division by 0

    SUM(CASE
    WHEN  d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
    'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
    AND fds.distance_to_fiber > 750
    THEN
      CASE WHEN ftg.median_total_district_cost <= 30000
        THEN 1 ELSE 0 END
      ELSE 0 END)::numeric
      /SUM(CASE WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
      'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
      AND fds.distance_to_fiber > 750
        THEN 1 ELSE 0 END)::numeric END
     AS low_cost_state_match_pct, --no fiber on block, in state match, district share less than 30k

  CASE WHEN SUM(CASE
    WHEN d.state_code not in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
    'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
    AND fds.distance_to_fiber > 750
      THEN 1 ELSE 0 END)::numeric = 0 then 0 ELSE --remove division by 0

  SUM(CASE
    WHEN d.state_code not in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
    'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
    AND fds.distance_to_fiber > 750
    AND (ftg.median_total_cost *
    CASE WHEN d.c1_discount_rate is null
    then .7 ELSE d.c1_discount_rate END)::numeric <= 30000
      THEN 1 ELSE 0
  END)::numeric
  / SUM(CASE
    WHEN d.state_code not in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
    'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
    AND fds.distance_to_fiber > 750
      THEN 1 ELSE 0 END)::numeric
  END AS low_build_cost_no_match --no state match, no fiber on block,  district share less than 30k

FROM ps.districts d

LEFT JOIN ps.districts_fiber df
ON d.district_id = df.district_id
AND d.funding_year = df.funding_year

LEFT JOIN ps.campuses c
ON d.district_id = c.district_id
AND d.funding_year = c.funding_year

JOIN ps.fiber_distance_static fds
ON c.campus_id =fds.campus_id
AND c.funding_year = fds.funding_year

LEFT JOIN ps.districts_sp_assignments sp
ON d.district_id = sp.district_id
AND d.funding_year = sp.funding_year

LEFT JOIN ftg
ON c.campus_id =ftg.campus_id
AND c.funding_year = 2019

WHERE d.district_type = 'Traditional'
AND d.in_universe = true
AND d.funding_year = 2019
AND df.fiber_target_status = 'Target'
AND (sp.primary_sp IN ('AT&T','CenturyLink','Comcast','Spectrum')
OR sp.primary_sp ILIKE '%Verizon%')
AND d.state_code Not IN ('AK','DC')

GROUP BY d.state_code


),

national_unscalable_table as ( --same calculations as state table, but grouped nationally
SELECT
COUNT(*) as unscalable_w_data,
SUM(CASE WHEN fds.distance_to_fiber <= 750 THEN 1 ELSE 0 END)
  /COUNT(CASE WHEN d.state_code Not IN ('AK','DC') THEN 1 END)::numeric
  AS fiber_at_door_pct,

CASE WHEN SUM(CASE WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
AND fds.distance_to_fiber > 750
AND d.c1_discount_rate IS NOT NULL
  THEN 1 ELSE 0 END) = 0 THEN 0 ELSE --remove division by 0

  SUM(CASE
  WHEN  d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
  'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
  AND fds.distance_to_fiber > 750
  THEN
    CASE WHEN ftg.median_total_district_cost <= 30000
      THEN 1 ELSE 0 END
    ELSE 0 END)::numeric
    /SUM(CASE WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
    'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
    AND fds.distance_to_fiber > 750
      THEN 1 ELSE 0 END)::numeric END
   AS low_cost_state_match_pct, --no fiber on block, in state match, district share less than 30k

CASE WHEN SUM(CASE
  WHEN d.state_code not in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
  'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
  AND fds.distance_to_fiber > 750
    THEN 1 ELSE 0 END)::numeric = 0 then 0 ELSE --remove division by 0

SUM(CASE
  WHEN d.state_code not in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
  'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
  AND fds.distance_to_fiber > 750
  AND (ftg.median_total_cost *
  CASE WHEN d.c1_discount_rate is null
  then .7 ELSE d.c1_discount_rate END)::numeric <= 30000
    THEN 1 ELSE 0
END)::numeric
/ SUM(CASE
  WHEN d.state_code not in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
  'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
  AND fds.distance_to_fiber > 750
    THEN 1 ELSE 0 END)::numeric
END AS low_build_cost_no_match --no state match, no fiber on block,  district share less than 30k

FROM ps.districts d

LEFT JOIN ps.districts_fiber df
ON d.district_id = df.district_id
AND d.funding_year = df.funding_year

LEFT JOIN ps.campuses c
ON d.district_id = c.district_id
AND d.funding_year = c.funding_year

JOIN ps.fiber_distance_static fds
ON c.campus_id =fds.campus_id
AND c.funding_year = fds.funding_year

LEFT JOIN ps.districts_sp_assignments sp
ON d.district_id = sp.district_id
AND d.funding_year = sp.funding_year

LEFT JOIN ftg
ON c.campus_id =ftg.campus_id
AND c.funding_year = 2019

WHERE d.district_type = 'Traditional'
AND d.in_universe = true
AND d.funding_year = 2019
AND df.fiber_target_status = 'Target'
AND (sp.primary_sp IN ('AT&T','CenturyLink','Comcast','Spectrum')
OR sp.primary_sp ILIKE '%Verizon%')
AND d.state_code Not IN ('AK','DC')

),


/*many of the states have a poorly sized sample so we do not want to
use national extrapolations instead of state extrapolations in many cases*/
sample_breakdown AS (
SELECT
d.state_code,
CASE WHEN sut.unscalable_w_data IS NULL THEN 0 ELSE sut.unscalable_w_data END as unscalable_w_data,
SUM(df.known_unscalable_campuses + df.assumed_unscalable_campuses) as total_unscalable_campuses,
SUM(d.num_campuses) as total_campuses,
CASE WHEN (SUM(df.known_unscalable_campuses + df.assumed_unscalable_campuses) = 0
          OR sut.unscalable_w_data = 0) THEN 0 ELSE
sut.unscalable_w_data /
SUM(df.known_unscalable_campuses + df.assumed_unscalable_campuses) END AS percentage_w_data
FROM ps.districts d

LEFT JOIN ps.districts_fiber df
ON d.district_id = df.district_id
AND d.funding_year = df.funding_year

LEFT JOIN state_unscalable_table sut
ON d.state_code = sut.state_code
AND d.funding_year = 2019

WHERE d.funding_year = 2019
AND d.in_universe = True
AND d.district_type = 'Traditional'
AND d.state_code Not IN ('AK','DC')

GROUP BY d.state_code,
sut.unscalable_w_data

ORDER BY 4 DESC

),

state_extrap AS (

  SELECT sb.state_code,
        sb.total_unscalable_campuses,
        sb.total_campuses,
        CASE WHEN sb.state_code = 'AK' then 0
        WHEN (sb.percentage_w_data > .2
        OR sb.unscalable_w_data >= 7)
          THEN sut.fiber_at_door_pct
          ELSE nut.fiber_at_door_pct
        END AS  fiber_at_door_pct,
        CASE WHEN sb.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
           'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI') THEN
          CASE
           WHEN (sb.percentage_w_data > .2
           OR sb.unscalable_w_data >= 7)
            THEN sut.low_cost_state_match_pct
            ELSE nut.low_cost_state_match_pct
           END
        ELSE 0 END AS  low_cost_match_pct,
        CASE WHEN sb.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
           'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI') THEN
         CASE
          WHEN (sb.percentage_w_data > .2
           OR sb.unscalable_w_data >= 7)
            THEN sut.low_build_cost_no_match
          ELSE nut.low_build_cost_no_match
        END
      ELSE 0 END AS  low_build_cost_no_match
  FROM
  sample_breakdown sb

  lEFT JOIN state_unscalable_table sut
  ON sb.state_code = sut.state_code

  JOIN national_unscalable_table nut
  ON true
)

SELECT se.state_code,
      ROUND(se.total_unscalable_campuses,2) as total_unscalable_campuses,
      ROUND((se.total_unscalable_campuses * se.fiber_at_door_pct)
       + (
          se.total_unscalable_campuses * (1-se.fiber_at_door_pct)
          * CASE WHEN se.low_cost_match_pct = 0
            THEN se.low_build_cost_no_match
            ELSE se.low_cost_match_pct END
          ),2)
      AS campuses_upgraded,
      ROUND(se.total_unscalable_campuses - ((se.total_unscalable_campuses * se.fiber_at_door_pct)
       + (
          se.total_unscalable_campuses * (1-se.fiber_at_door_pct)
          * CASE WHEN se.low_cost_match_pct = 0
            THEN se.low_build_cost_no_match
            ELSE se.low_cost_match_pct END
          )),2)
      AS remaining_unscalable_campuses,
      ROUND(1-(se.total_unscalable_campuses/se.total_campuses),3)

      AS current_percent_campuses_on_fiber,
    ROUND(1-((se.total_unscalable_campuses - ((se.total_unscalable_campuses * se.fiber_at_door_pct)
     + (
        se.total_unscalable_campuses * (1-se.fiber_at_door_pct)
        * CASE WHEN se.low_cost_match_pct = 0
          THEN se.low_build_cost_no_match
          ELSE se.low_cost_match_pct END
        )))/ se.total_campuses ),3)
      AS future_percent_campuses_on_fiber
FROM state_extrap se
