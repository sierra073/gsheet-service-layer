with state_agg as (
    SELECT d.funding_year,
      d.state_code,
      SUM(
        CASE
          WHEN d.locale IN ('Rural','Town')
            THEN 1
          ELSE 0
        END)
      / COUNT(*)::numeric AS pct_rural,
      SUM(
        CASE
          WHEN d.size IN ('Tiny','Small')
            THEN 1
          ELSE 0
        END)
      / COUNT(*)::numeric AS pct_small,
--Correcting bw for large/megas
    MEDIAN(CASE
      WHEN fit.fit_for_ia = False
        THEN NULL
      WHEN d.size IN ('Tiny','Small')
        THEN bw.ia_bandwidth_per_student_kbps
      WHEN d.size IN ('Medium','Large','Mega')
        THEN bw.ia_bandwidth_per_student_kbps * .7
    END
    / CASE
      WHEN d.size IN ('Tiny','Small')
        THEN 1000
      WHEN d.size IN ('Medium','Large','Mega')
        THEN 700
    END) AS med_pct_of_meeting_goal
    FROM ps.districts d

    JOIN ps.districts_bw_cost bw
    ON d.district_id = bw.district_id
    AND d.funding_year = bw.funding_year

    JOIN ps.districts_fit_for_analysis fit
    ON d.district_id = fit.district_id
    AND d.funding_year = fit.funding_year

    WHERE d.in_universe = True
    AND d.district_type = 'Traditional'

    GROUP BY d.funding_year,
      d.state_code
)

SELECT bw.state_code,
      bw.funding_year,
      (bw.districts_meeting_2018_goal_oversub::numeric /s.districts_clean_ia_sample::numeric)
      * s.districts_population::numeric as districts_meeting_2018_goal_oversub,
      (bw.districts_not_meeting_2018_goal_oversub::numeric  /s.districts_clean_ia_sample::numeric )
      * s.districts_population::numeric  as districts_not_meeting_2018_goal_oversub,
      bw.districts_meeting_2018_goal_oversub::numeric /s.districts_clean_ia_sample::numeric  as pct_districts_meeting_1mbps,
      bw.districts_meeting_2018_goal_oversub::numeric /s.districts_clean_ia_sample::numeric
      - lybw.districts_meeting_2018_goal_oversub::numeric /lys.districts_clean_ia_sample::numeric as increase_pct_dist_meeting_1mbps,
      MAX(bw.districts_meeting_2018_goal_oversub::numeric /s.districts_clean_ia_sample::numeric
      - lybw.districts_meeting_2018_goal_oversub::numeric /lys.districts_clean_ia_sample::numeric) OVER(PARTITION BY bw.state_code) as max_year_pct_increase,
      (bw.students_meeting_2018_goal_oversub::numeric  /s.students_clean_ia_sample::numeric )
      * s.students_population::numeric  as students_meeting_2018_goal_oversub,
      (bw.students_not_meeting_2018_goal_oversub::numeric  /s.students_clean_ia_sample::numeric )
      * s.students_population::numeric  as students_not_meeting_2018_goal_oversub,
      bw.students_meeting_2018_goal_oversub::numeric /s.students_clean_ia_sample::numeric  as pct_students_meeting_1mbps,
      bw.median_ia_bandwidth_per_student_kbps,
      bw.median_ia_monthly_cost_per_mbps,
      s.districts_population,
      s.students_population,
      sag.pct_rural,
      sag.pct_small,
      sag.med_pct_of_meeting_goal,
      CASE
        WHEN (ss.procurement ILIKE '%State%'
        OR ss.state_code IN ('AR','IA','RI','UT'))
          THEN 'State Procured'
        WHEN ss.procurement = 'Regional procurement'
          THEN 'Regional procurement'
        WHEN (ss.procurement = 'District-procured'
        OR ss.state_code = 'CT')
          THEN 'District-procured'
        ELSE NULL
      END AS procurement_pattern,
      
      --Manually add Boolean on years whether State_Init is active
      case when bw.state_code = 'AK' and s.funding_year in (2018,2019)
      then true 
      when bw.state_code = 'AZ' and s.funding_year in (2016,2017,2018,2019)
      then true
      when bw.state_code = 'CA' and s.funding_year in (2017,2018,2019)
      then true
      when bw.state_code = 'CO' and s.funding_year in (2016,2017,2018,2019)
      then true
      when bw.state_code = 'IL' and s.funding_year in (2016,2017,2018,2019)
      then true
      when bw.state_code = 'IN' and s.funding_year in (2017,2018,2019)
      then true
      when bw.state_code = 'KS' and s.funding_year in (2016,2017,2018,2019)
      then true
      when bw.state_code = 'LA' and s.funding_year in (2017,2018,2019)
      then true
      when bw.state_code = 'MA' and s.funding_year in (2016,2017,2018,2019)
      then true
      when bw.state_code = 'MN' and s.funding_year in (2016,2017,2018,2019)
      then true
      when bw.state_code = 'MO' and s.funding_year in (2017,2018,2019)
      then true
      when bw.state_code = 'MT' and s.funding_year in (2015,2016,2017,2018,2019)
      then true
      when bw.state_code = 'NH' and s.funding_year in (2015,2016,2017,2018,2019)
      then true
      when bw.state_code = 'NJ' and s.funding_year in (2017,2018,2019)
      then true
      when bw.state_code = 'NM' and s.funding_year in (2015,2016,2017,2018,2019)
      then true
      when bw.state_code = 'NV' and s.funding_year in (2017,2018,2019)
      then true
      when bw.state_code = 'OK' and s.funding_year in (2016,2017,2018,2019)
      then true
      when bw.state_code = 'TX' and s.funding_year in (2016,2017,2018,2019)
      then true
      when bw.state_code = 'VA' and s.funding_year in (2014,2015,2016,2017,2018,2019)
      then true
      when bw.state_code = 'WA' and s.funding_year in (2017,2018,2019)
      then true
      when bw.state_code = 'WY' and s.funding_year in (2016,2017,2018,2019)
      then true
      
      else false
      end as active_initiative
      
FROM ps.states_bw_cost bw

JOIN ps.states s
ON bw.state_code = s.state_code
AND bw.funding_year = s.funding_year
AND bw.data_status = s.data_status

JOIN state_agg sag
ON bw.state_code = sag.state_code
AND bw.funding_year = sag.funding_year

LEFT JOIN ps.states_static ss
ON bw.state_code = ss.state_code

LEFT JOIN ps.states_bw_cost lybw
ON bw.state_code = lybw.state_code
AND bw.funding_year = lybw.funding_year + 1
AND bw.data_status = lybw.data_status

LEFT JOIN ps.states lys
ON bw.state_code = lys.state_code
AND bw.funding_year = lys.funding_year + 1
AND bw.data_status = lys.data_status

WHERE bw.data_status = 'current'

ORDER BY s.state_code,
s.funding_year