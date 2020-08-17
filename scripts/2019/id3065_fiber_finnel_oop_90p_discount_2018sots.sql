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
funnel_metric AS (
SELECT
  fds.campus_id,
  fds.funding_year,
  fds.distance_to_fiber,
  CASE
    WHEN d.state_code IN ('AZ','IL','MO','WA')
      THEN 0
    WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                    'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
      THEN ftg.median_total_district_cost
    ELSE ftg.median_total_cost
      * .1 END AS metric
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
),

unscalable_campus_sample AS ( --sums all unscalable campuses and removes AK for extrapolations
  SELECT
  --d.state_code,
  SUM(CASE
        WHEN d.state_code = 'AK'
        THEN ftg.median_total_cost
            * .1
          ELSE NULL
      END) AS ak_sum_unscalable_campuses,--(147)
  SUM(CASE
        WHEN d.state_code != 'AK'
        THEN CASE
          WHEN d.state_code IN ('AZ','IL','MO','WA')
            THEN 0
          WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
            THEN ftg.median_total_district_cost
          ELSE ftg.median_total_cost
            * .1 END
          ELSE NULL
      END) AS nat_sum_unscalable_campuses_no_ak --(1210)
  FROM ps.campuses c

      JOIN ftg
      ON c.campus_id = ftg.campus_id

      LEFT JOIN ps.districts d
      ON c.district_id = d.district_id
      AND c.funding_year = d.funding_year

      LEFT JOIN ps.districts_sp_assignments sp
      ON d.district_id = sp.district_id
      AND d.funding_year = sp.funding_year

      LEFT JOIN ps.districts_fiber df
      ON d.district_id = df.district_id
      AND d.funding_year = df.funding_year


  WHERE d.district_type = 'Traditional'
  AND d.in_universe = true
  AND d.funding_year = 2019

  --GROUP BY d.state_code
),
fiber_on_block_sample as ( /*this is the sample group from distances_fiber_static that we trust the distances fiber locator provided
  used for calculating fiber on the block incumbent distances are used if they are withinng 750 feet
   as that already counts as fiber on the block*/
  SELECT
  SUM(CASE
        WHEN sp.primary_sp NOT IN ('AT&T','CenturyLink','Comcast','Spectrum')
        AND sp.primary_sp NOT ILIKE '%Verizon%'
        AND fds.distance_to_fiber <= 200
          THEN fm.metric
        ELSE 0 END)::numeric
  /SUM(CASE
        WHEN (sp.primary_sp NOT IN ('AT&T','CenturyLink','Comcast','Spectrum')
        AND sp.primary_sp NOT ILIKE '%Verizon%')
          THEN fm.metric
        ELSE 0 END)::numeric as no_inc_pct_fiber_at_door, --(.067)
  SUM(CASE
        WHEN sp.primary_sp NOT IN ('AT&T','CenturyLink','Comcast','Spectrum')
        AND sp.primary_sp NOT ILIKE '%Verizon%'
        AND fds.distance_to_fiber <= 750
          THEN fm.metric
        ELSE 0 END)::numeric
  /SUM(CASE
        WHEN (sp.primary_sp NOT IN ('AT&T','CenturyLink','Comcast','Spectrum')
        AND sp.primary_sp NOT ILIKE '%Verizon%')
  THEN fm.metric ELSE 0 END)::numeric  as no_inc_pct_fiber_on_block, --(.168)
  SUM(CASE
        WHEN (sp.primary_sp IN ('AT&T','CenturyLink','Comcast','Spectrum') OR sp.primary_sp ILIKE '%Verizon%')
        AND fds.distance_to_fiber <= 750
         THEN fm.metric
        ELSE 0 END)::numeric
  / SUM(CASE
          WHEN (sp.primary_sp IN ('AT&T','CenturyLink','Comcast','Spectrum')
          OR sp.primary_sp ILIKE '%Verizon%')
            THEN fm.metric
          ELSE 0 END)::numeric AS inc_pct_fiber_on_block, --(.285)
  SUM(CASE
        WHEN fds.distance_to_fiber <= 750
          THEN fm.metric
        ELSE 0 END)::numeric
  /SUM(CASE
        WHEN (sp.primary_sp NOT IN ('AT&T','CenturyLink','Comcast','Spectrum')
        AND sp.primary_sp NOT ILIKE '%Verizon%')
        OR sp.primary_sp IS NULL
          THEN fm.metric
        WHEN(sp.primary_sp IN ('AT&T','CenturyLink','Comcast','Spectrum')
        OR sp.primary_sp ILIKE '%Verizon%')
        AND fds.distance_to_fiber <= 750
          THEN fm.metric
        ELSE 0 END)::numeric AS hybrid_pct_fiber_on_block  -- (.256)
                                                      /*we don't trust when the primary sp is one of the incumbents
                                                      because we think fiber is closer. If we already see fiber on the block
                                                      for one of those incumbents, we can assume they have fiber at the door
                                                      so this metric combines those. THIS IS THE PERCENT USED GOING FORWARD
                                                      the rest are informational*/
  FROM ps.fiber_distance_static fds

  LEFT JOIN ps.campuses c
  ON fds.campus_id = c.campus_id
  AND fds.funding_year = c.funding_year

  LEFT JOIN ps.districts d
  ON c.district_id = d.district_id
  AND c.funding_year = d.funding_year

  LEFT JOIN ps.districts_sp_assignments sp
  ON d.district_id = sp.district_id
  AND d.funding_year = sp.funding_year

  LEFT JOIN ps.districts_fiber df
  ON d.district_id = df.district_id
  AND d.funding_year = df.funding_year

  JOIN funnel_metric fm
  ON fds.campus_id = fm.campus_id
  AND fds.funding_year = fm.funding_year

  WHERE d.district_type = 'Traditional'
  AND d.in_universe = true
  AND d.funding_year = 2019
  AND df.fiber_target_status = 'Target'
  AND d.state_code NOT IN ('DC','AK')
),

build_cost_categories AS (
SELECT

--Sample Sizes
  COUNT(*) AS all_sample, --(854)
  COUNT(CASE
        WHEN (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
          THEN 1
  END) AS needs_build_sample, --(687)
  COUNT(CASE
        WHEN fds.distance_to_fiber <= 750
          THEN 1
      END) AS fob_sample, --(167)
--State Match Percent Iterations
  SUM(CASE
        WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(fm.metric)::numeric AS state_match_all, --(.711)
  SUM(CASE
        WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(CASE
        WHEN (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
          THEN fm.metric
          ELSE 0 END)::numeric AS state_match_needs_build, --(.731)
  SUM(CASE
        WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND fds.distance_to_fiber <= 750
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(CASE
        WHEN fds.distance_to_fiber <= 750
          THEN fm.metric
          ELSE 0 END)::numeric AS state_match_fob, --(.628)

--Free State Iterations IL,MO,WA have free state matches
  --1 AS state_full_match_hard, --(1)
  SUM(CASE
        WHEN d.state_code IN ('AZ','IL','MO','WA')
          THEN fm.metric
          ELSE 0 END)
  /SUM(fm.metric)::numeric AS state_full_match_all, --(.123)
  SUM(CASE
        WHEN d.state_code IN ('AZ','IL','MO','WA')
        AND (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
          THEN fm.metric
          ELSE 0 END)
  /SUM(CASE
        WHEN(fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
          THEN fm.metric
          ELSE 0 END)::numeric AS state_full_match_needs_build, --(.116)
  SUM(CASE
        WHEN d.state_code IN ('AZ','IL','MO','WA')
        AND fds.distance_to_fiber <= 750
          THEN fm.metric
          ELSE 0 END)
  /SUM(CASE
        WHEN fds.distance_to_fiber <= 750
          THEN fm.metric
          ELSE 0 END)::numeric AS state_full_match_fob, --(.150)

  --free state match due to low discount rate
  SUM(CASE
        WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND d.state_code NOT IN ('AZ','IL','MO','WA')
        AND ftg.median_total_district_cost = 0
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(fm.metric)::numeric AS free_state_match_all, --(.376)
  SUM(CASE
        WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND d.state_code NOT IN ('AZ','IL','MO','WA')
        AND (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
        AND ftg.median_total_district_cost = 0
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(CASE
        WHEN (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
          THEN fm.metric
          ELSE 0
        END)::numeric AS free_state_match_needs_build, --(.393)
  SUM(CASE
        WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND d.state_code NOT IN ('AZ','IL','MO','WA')
        AND fds.distance_to_fiber <= 750
        AND ftg.median_total_district_cost = 0
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(CASE
        WHEN fds.distance_to_fiber <= 750
          THEN fm.metric
          ELSE 0
        END)::numeric AS free_state_match_fob, --(.305)

    --low cost state match due to build cost or lowish discount rate
    SUM(CASE
          WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND d.state_code NOT IN ('AZ','IL','MO','WA')
          AND ftg.median_total_district_cost > 0
          AND ftg.median_total_district_cost <= 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(fm.metric)::numeric AS low_cost_state_match_all, --(.043)
    SUM(CASE
          WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND d.state_code NOT IN ('AZ','IL','MO','WA')
          AND (fds.distance_to_fiber > 750
          OR fds.distance_to_fiber IS NULL)
          AND ftg.median_total_district_cost > 0
          AND ftg.median_total_district_cost <= 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(CASE
          WHEN (fds.distance_to_fiber > 750
          OR fds.distance_to_fiber IS NULL)
            THEN fm.metric
            ELSE 0 END)::numeric AS low_cost_state_match_needs_build, --(.049)
    SUM(CASE
          WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND d.state_code NOT IN ('AZ','IL','MO','WA')
          AND fds.distance_to_fiber <= 750
          AND ftg.median_total_district_cost > 0
          AND ftg.median_total_district_cost <= 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(CASE
          WHEN fds.distance_to_fiber <= 750
            THEN fm.metric
            ELSE 0
          END)::numeric AS low_cost_state_match_fob, --(.018)

    --State Match High costs due to build or low discount rate
    SUM(CASE
          WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND d.state_code NOT IN ('AZ','IL','MO','WA')
          AND ftg.median_total_district_cost > 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(fm.metric)::numeric AS high_cost_state_match_all, --(.169)
    SUM(CASE
          WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND d.state_code NOT IN ('AZ','IL','MO','WA')
          AND (fds.distance_to_fiber > 750
          OR fds.distance_to_fiber IS NULL)
          AND ftg.median_total_district_cost > 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(CASE
          WHEN (fds.distance_to_fiber > 750
          OR fds.distance_to_fiber IS NULL)
            THEN fm.metric
            ELSE 0 END)::numeric AS high_cost_state_match_needs_build, --(.172)
    SUM(CASE
          WHEN d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND d.state_code NOT IN ('AZ','IL','MO','WA')
          AND fds.distance_to_fiber <= 750
          AND ftg.median_total_district_cost > 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(CASE
          WHEN fds.distance_to_fiber <= 750
            THEN fm.metric
          ELSE 0 END)::numeric AS high_cost_state_match_fob, --(.156)

    -- low cost NO state match due to build cost or low discount rate
    SUM(CASE
          WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND (ftg.median_total_cost
                * .1) <= 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(fm.metric)::numeric AS low_cost_no_match_all, --(.004)
    SUM(CASE
          WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND (fds.distance_to_fiber > 750
          OR fds.distance_to_fiber IS NULL)
          AND (ftg.median_total_cost
                * .1) <= 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(CASE
          WHEN (fds.distance_to_fiber > 750
          OR fds.distance_to_fiber IS NULL)
            THEN fm.metric
            ELSE 0 END)::numeric AS low_cost_no_match_needs_build, --(.003)
    SUM(CASE
          WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND fds.distance_to_fiber <= 750
          AND (ftg.median_total_cost
                * .1) <= 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(CASE
          WHEN fds.distance_to_fiber <= 750
            THEN fm.metric
            ELSE 0
          END)::numeric AS low_cost_no_match_fob, --(.006)

    -- High cost NO state match due to build cost or high discount rate
    SUM(CASE
          WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND (ftg.median_total_cost
                * .1) > 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(fm.metric)::numeric AS high_cost_no_match_all, --(.285)
    SUM(CASE
          WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND (fds.distance_to_fiber > 750
          OR fds.distance_to_fiber IS NULL)
          AND (ftg.median_total_cost
                * .1) > 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(CASE
          WHEN (fds.distance_to_fiber > 750
          OR fds.distance_to_fiber IS NULL)
            THEN fm.metric
            ELSE 0 END)::numeric AS high_cost_no_match_needs_build, --(.266)
    SUM(CASE
          WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                            'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
          AND fds.distance_to_fiber <= 750
          AND (ftg.median_total_cost
                * .1) > 30000
            THEN fm.metric
            ELSE 0
        END)::numeric
    /SUM(CASE
          WHEN fds.distance_to_fiber <= 750
            THEN fm.metric
            ELSE 0
          END)::numeric AS high_cost_no_match_fob, --(.365)

  -- Calculates Low cost for no state match states if they were to have a match
  SUM(CASE
        WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND ftg.median_total_district_cost <= 30000
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(fm.metric)::numeric AS low_cost_if_had_match_all, --(.142)
  SUM(CASE
        WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
        AND ftg.median_total_district_cost <= 30000
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(CASE
        WHEN (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
          THEN fm.metric
          ELSE 0 END)::numeric AS low_cost_if_had_match_needs_build, --(.121)
  SUM(CASE
        WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND fds.distance_to_fiber <= 750
        AND ftg.median_total_district_cost <= 30000
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(CASE
        WHEN fds.distance_to_fiber <= 750
          THEN fm.metric
          ELSE 0
        END)::numeric AS low_cost_if_had_match_fob, --(.228)

  -- Calculates High cost for no state match states if they were to have a match
  SUM(CASE
        WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND ftg.median_total_district_cost > 30000
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(fm.metric)::numeric AS high_cost_if_had_match_all, --(.148)
  SUM(CASE
        WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
        AND ftg.median_total_district_cost > 30000
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(CASE
        WHEN (fds.distance_to_fiber > 750
        OR fds.distance_to_fiber IS NULL)
          THEN fm.metric
          ELSE 0 END)::numeric AS high_cost_if_had_match_needs_build, --(.148)
  SUM(CASE
        WHEN d.state_code NOT in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
        AND fds.distance_to_fiber <= 750
        AND ftg.median_total_district_cost > 30000
          THEN fm.metric
          ELSE 0
      END)::numeric
  /SUM(CASE
        WHEN fds.distance_to_fiber <= 750
          THEN fm.metric
          ELSE 0
        END)::numeric AS high_cost_if_had_match_fob --(.144)



  FROM ps.fiber_distance_static fds

  LEFT JOIN ps.campuses c
  ON fds.campus_id = c.campus_id
  AND fds.funding_year = c.funding_year

  JOIN ftg
  ON fds.campus_id = ftg.campus_id

  LEFT JOIN ps.districts d
  ON c.district_id = d.district_id
  AND c.funding_year = d.funding_year

  LEFT JOIN ps.districts_sp_assignments sp
  ON d.district_id = sp.district_id
  AND d.funding_year = sp.funding_year

  LEFT JOIN ps.districts_fiber df
  ON d.district_id = df.district_id
  AND d.funding_year = df.funding_year

  JOIN funnel_metric fm
  ON fds.campus_id = fm.campus_id
  AND fds.funding_year = fm.funding_year


  WHERE d.district_type = 'Traditional'
  AND d.in_universe = true
  AND d.funding_year = 2019
  AND df.fiber_target_status = 'Target'
  AND d.state_code NOT IN ('DC','AK')
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

--above is not the full fiber funnel, Just up to the build_cost_categories table
SELECT --ucs.state_code,

      --First Layer of Funnel (fiber on block)
      ucs.ak_sum_unscalable_campuses AS unscalable_ak, --(147)
      ucs.nat_sum_unscalable_campuses_no_ak * fob.hybrid_pct_fiber_on_block AS fiber_on_block, --(310)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block) AS needs_build, --(900)

      --Second Layer of Funnel (State Match)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * bcc.state_match_needs_build AS state_match, --(658)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * (1 - bcc.state_match_needs_build) AS no_match, --(242)

      --Third Layer of Funnel (build costs)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * bcc.state_full_match_needs_build
      AS state_full_match, --(105) IL,MO,WA state give free builds to all campuses
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * bcc.free_state_match_needs_build AS free_state_match,--(354)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * bcc.low_cost_state_match_needs_build AS low_cost_state_match, --(45)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * bcc.high_cost_state_match_needs_build AS high_cost_state_match, --(154)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * bcc.low_cost_no_match_needs_build AS low_cost_no_match,--(2)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * bcc.high_cost_no_match_needs_build AS high_cost_no_match, --(240)

      --Fourth Layer of Funnel (scalable Non fiber)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * bcc.high_cost_no_match_needs_build * nfs.percent_non_cable_wireless AS unscalable_non_fiber, --(47)
      ucs.nat_sum_unscalable_campuses_no_ak * (1 - fob.hybrid_pct_fiber_on_block)
      * bcc.high_cost_no_match_needs_build * (1 - nfs.percent_non_cable_wireless) AS scalable_non_fiber--(193)
FROM unscalable_campus_sample ucs,
    fiber_on_block_sample fob,
    build_cost_categories bcc,
    non_fiber_scalable nfs
