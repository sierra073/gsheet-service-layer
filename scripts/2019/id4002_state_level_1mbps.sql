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
    END) AS med_pct_of_meeting_goal,
    SUM(CASE
      WHEN fit.fit_for_ia_cost = True
        THEN bw.ia_monthly_cost_total
      ELSE NULL
    END) /
    SUM(CASE
      WHEN fit.fit_for_ia_cost = True
        THEN d.num_students
      ELSE NULL
    END) AS ia_dollars_per_student,
    COUNT(DISTINCT sp.primary_sp) AS num_service_providers_per_state
    FROM ps.districts d

    JOIN ps.districts_bw_cost bw
    ON d.district_id = bw.district_id
    AND d.funding_year = bw.funding_year

    JOIN ps.districts_sp_assignments sp
    ON d.district_id = sp.district_id
    AND d.funding_year = sp.funding_year

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
      (bw.median_ia_bandwidth_per_student_kbps - bw15.median_ia_bandwidth_per_student_kbps)
      / bw15.median_ia_bandwidth_per_student_kbps
      AS pct_increase_bw_per_student_since_2015,
      bw.median_ia_monthly_cost_per_mbps,
      s.districts_population,
      s.students_population,
      sag.pct_rural,
      sag.pct_small,
      sag.med_pct_of_meeting_goal,
      sag.ia_dollars_per_student,
      sag.ia_dollars_per_student
      /sag15.ia_dollars_per_student
      AS pct_of_2015_ia_dollars_per_student,
      sag.num_service_providers_per_state,
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
      END AS procurement_pattern
FROM ps.states_bw_cost bw

JOIN ps.states s
ON bw.state_code = s.state_code
AND bw.funding_year = s.funding_year
AND bw.data_status = s.data_status

JOIN state_agg sag
ON bw.state_code = sag.state_code
AND bw.funding_year = sag.funding_year

JOIN state_agg sag15
ON bw.state_code = sag15.state_code
AND sag15.funding_year = 2015

JOIN ps.states_bw_cost bw15
ON bw.state_code = bw15.state_code
AND bw15.funding_year = 2015
AND bw.data_status = bw15.data_status

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
