with medians as (
    SELECT 
        d.state_code,
        MEDIAN(CASE
                WHEN dbc.meeting_2018_goal_oversub = TRUE
                  THEN dbc.ia_monthly_cost_per_mbps
                ELSE NULL
              END) as median_district_meeting1mbps_internetcost_monthlypermbps,
        MEDIAN(CASE
                WHEN dbc.meeting_2018_goal_oversub = FALSE
                AND dl.most_recent_ia_contract_end_date <= '2020-06-30'
                  THEN dbc.ia_monthly_cost_per_mbps
                ELSE NULL
              END) as median_district_under1mbps_contractexpiry_internetcost_monthlypermbps

    FROM ps.districts d

    JOIN ps.districts_fit_for_analysis dffa
    ON d.district_id = dffa.district_id
    AND d.funding_year = dffa.funding_year

    JOIN ps.districts_bw_cost dbc
    ON d.district_id = dbc.district_id
    AND d.funding_year = dbc.funding_year

    JOIN ps.districts_lines dl
    ON d.district_id = dl.district_id
    AND d.funding_year = dl.funding_year

    WHERE d.in_universe = TRUE
    AND d.district_type = 'Traditional'
    and d.funding_year= 2019
    and d.state_code in ('MN', 'NY', 'OH', 'PA', 'TX')
    and dffa.fit_for_ia_cost = TRUE
    
    GROUP BY d.state_code
)

    SELECT 
      d.state_code,
      SUM(CASE
            WHEN dffa.fit_for_ia = TRUE 
            AND dbc.meeting_2018_goal_oversub = FALSE
            AND dl.most_recent_ia_contract_end_date <= '2020-06-30'
              THEN 1
            ELSE 0
          END) AS districts_under1mbps_contractexpiry_sample,
      m.median_district_meeting1mbps_internetcost_monthlypermbps,
      m.median_district_under1mbps_contractexpiry_internetcost_monthlypermbps,
      SUM(CASE
            WHEN dffa.fit_for_ia_cost = TRUE 
            AND dbc.meeting_2018_goal_oversub = FALSE
            AND dl.most_recent_ia_contract_end_date <= '2020-06-30'
            AND dbc.ia_monthly_cost_total / m.median_district_meeting1mbps_internetcost_monthlypermbps >=
                dbc.projected_bw_fy2018
              THEN 1
            ELSE 0
          END) AS districts_under1mbps_contractexpiry_needstatemedian_sample,
      SUM(CASE
            WHEN dffa.fit_for_ia_cost = TRUE 
            AND dbc.meeting_2018_goal_oversub = FALSE
            AND dl.most_recent_ia_contract_end_date <= '2020-06-30'
            AND dbc.ia_monthly_cost_total / m.median_district_meeting1mbps_internetcost_monthlypermbps >=
                dbc.projected_bw_fy2018
              THEN 1::numeric
            ELSE 0::numeric
          END)/SUM(CASE
                    WHEN dffa.fit_for_ia_cost = TRUE 
                    AND dbc.meeting_2018_goal_oversub = FALSE
                    AND dl.most_recent_ia_contract_end_date <= '2020-06-30'
                      THEN 1::numeric
                    ELSE 0::numeric
                  END) as pctdistricts_under1mbps_contractexpiry_needstatemedian


    FROM ps.districts d

    JOIN ps.districts_fit_for_analysis dffa
    ON d.district_id = dffa.district_id
    AND d.funding_year = dffa.funding_year

    JOIN ps.districts_bw_cost dbc
    ON d.district_id = dbc.district_id
    AND d.funding_year = dbc.funding_year

    JOIN ps.districts_lines dl
    ON d.district_id = dl.district_id
    AND d.funding_year = dl.funding_year
    
    JOIN medians m
    on m.state_code = d.state_code

    WHERE d.in_universe = TRUE
    AND d.district_type = 'Traditional'
    and d.funding_year= 2019

    GROUP BY d.state_code,
    m.median_district_meeting1mbps_internetcost_monthlypermbps,
    m.median_district_under1mbps_contractexpiry_internetcost_monthlypermbps
    
    ORDER BY d.state_code
