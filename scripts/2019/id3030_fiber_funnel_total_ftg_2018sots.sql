with districts_num_campus_builds as (
  select
    district_id,
    count(distinct campus_id) as num_campus_builds

  from dm.campus_build_costs
  where total_cost_median_wan > 0
group by 1
),

district_build_costs as (
  select
    dbc.district_id,
    total_cost_ia::numeric/(case when num_campus_builds > 0 then num_campus_builds else 1 end) as total_cost_ia_distributed,
    total_erate_funding_ia::numeric/(case when num_campus_builds > 0 then num_campus_builds else 1 end) as total_erate_funding_ia_distributed,
    total_state_funding_ia::numeric/(case when num_campus_builds > 0 then num_campus_builds else 1 end) as total_state_funding_ia_distributed,
    total_district_funding_ia::numeric/(case when num_campus_builds > 0 then num_campus_builds else 1 end) as total_district_cost_ia_distributed

  from dm.district_build_costs dbc
  left join districts_num_campus_builds dnc
    on dbc.district_id = dnc.district_id
),

campus_build_costs as (
  select
    cbc.campus_id,
    cbc.district_id,
    median_total_cost_wan + (case when total_cost_ia_distributed > 0 then total_cost_ia_distributed else 0 end) as median_total_cost,
    median_total_erate_funding_wan + (case when total_erate_funding_ia_distributed > 0 then total_erate_funding_ia_distributed else 0 end) as median_total_erate_funding,
    median_total_state_funding_wan + (case when total_state_funding_ia_distributed > 0 then total_state_funding_ia_distributed else 0 end) as median_total_state_funding,
    case when c1_discount_rate = 0 then median_total_cost_wan + (case when total_cost_ia_distributed > 0 then total_cost_ia_distributed else 0 end)
    else median_total_district_funding_wan + (case when total_district_cost_ia_distributed > 0 then total_district_cost_ia_distributed else 0 end)
    end as median_total_district_cost

    from
    dm.campus_build_costs cbc
    left join district_build_costs dbc
      on cbc.district_id = dbc.district_id

    where total_cost_median_wan > 0
),

ftg AS (
select * from campus_build_costs order by district_id
),

--***FTG Query above***

 fiber_at_door as (
SELECT
SUM (CASE WHEN c.campus_id IN
        (13798,11875,17879,67201,65712,76406,107524,107587,13925,36784,43833,88755, 4398,77147,
        78736,79761,88370,43801,94080,3629,33528,42465,2253,7839,92739,44693,78581,48366,10407,
        31759,21607,36218,51945,8338,101226,7020,7342,78670,67202,78810,89465,33527,43511,52875,
        67758,24512,43025,78737,88436,78728,78694,8423,10401,106663,51948,42723,78713,78727,78760,
        78584,118048,36342,51816,43492,16052,43491,7919,36343,51988,66017,68417,78699,67555,92741,
        53493,34282,43693,51965,78827,34035,34036,43483,43486,106658,103973,78661,43805,89668)
      THEN 1
ELSE 0 END)/COUNT(c.campus_id)::numeric as percent_fiber_at_door,
SUM (CASE WHEN c.campus_id IN
        (13798,11875,17879,67201,65712,76406,107524,107587,13925,36784,43833,88755,4398,77147,78736,
        79761,88370,43801,94080,3629,33528,42465,2253,7839,92739,44693,78581,48366,10407,31759,21607,
        36218,51945,8338,101226,7020,7342,78670,67202,78810,89465,33527,43511,52875,67758,24512,43025,
        78737,88436,78728,78694,8423,10401,106663,51948,42723,78713,78727,78760,78584,118048,36342,51816,
        43492,16052,43491,7919,36343,51988,66017,68417,78699,67555,92741,53493,34282,43693,51965,78827,
        34035,34036,43483,43486,106658,103973,78661,43805,89668,8004,43444,31993,51876,78716,52874,
        116113,4396,22354,34798,87615,7874,43003,9623,51946,101228,11823,43582,36340,111188,51963,43475,
        78618,86926,14065,34918,53795,77532,51718,64226,5405,101276,4397,52061,93181,72171,52204,107166,
        15847,32000,6424,43270,66016,31946,52520,4924,52017,33795,4861,33796,41727,101277,17480,50290,
        31761,50627,52358,43126,79640,29431,52873,4868,100827,50482,1226,33499,52891,74235,108355,65483,
        32051,15469,33633,110254,52693,8198,31998,42467,53752,78777,78753,31762,87034,11827,12149,6423,53751,
        34661,43446,12139,51829,43034,31999,4987,35181,23394,38031,64295,103978,36337,36346,77131,2275,89306,
        31100,68215,111542,52012,101227,31925,17955,4654,64451,96148,12221,64450,88375,43438,54166,32938,17487)
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
      END)/SUM(ftg.median_total_cost) AS free_state_match_percent_w_exp,
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
