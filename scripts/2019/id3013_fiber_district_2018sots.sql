WITH  no_bid_districts as (
    SELECT rdlkp.district_id
    FROM ps.usac_line_items uli

    LEFT JOIN dwh.ft_usac_allocations ua
    ON uli.frn::varchar = ua.frn::varchar
    AND uli.funding_year = ua.funding_year

    LEFT JOIN dwh.recipients_districts_lkp rdlkp
    ON ua.recipient_id = rdlkp.recipient_id
    AND uli.funding_year = rdlkp.funding_year

    WHERE uli.funding_year = 2019
    AND rdlkp.district_id is not null
    --AND fiber_type is not null

    GROUP BY rdlkp.district_id

    HAVING SUM(uli.num_bids_received) = 0),

metrics as (
SELECT fw.funding_year,
      fw.district_id,
      fw.state_code,
      CASE WHEN fw.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
            THEN true
            ELSE false
      END AS state_match,
      fw.name,
      d.locale,
      CASE WHEN d.locale IN ('Rural','Town') THEN 'Rural'
        ELSE 'Urban'
      END as locale_grouped,
      d.ulocal,
      CASE WHEN d.ulocal::int IN (33,43) THEN true ELSE FALSE
        END AS super_remote_locale,
      d.size,
      CASE WHEN d.size in ('Tiny','Small') THEN 'Small'
        ELSE 'Large'
      END as size_grouped,
      d.num_students,
      CASE WHEN d.num_students <= 100 then true
        ELSE false
        END AS low_num_students,
      d.num_campuses,
      d.c1_discount_rate,
      CASE WHEN d.c1_discount_rate >= .8
        THEN true
        ELSE false
      END AS high_discount_rate,
      d.latitude::numeric,
      d.longitude::numeric,
      sp.primary_sp,
      fit.fit_for_ia,
      fib.hierarchy_ia_connect_category,
      CASE WHEN fib.hierarchy_ia_connect_category = 'Fiber'
          then true
          WHEN fib.hierarchy_ia_connect_category = 'None - Error'
          THEN NULL
        ELSE FALSE end as fiber_ia,
      bw.delayed_denied,
      CASE WHEN fw.fit_for_ia = true THEN bw.meeting_2014_goal_no_oversub
      ELSE FALSE END AS meeting_2014_goal_no_oversub,
      CASE WHEN fw.fit_for_ia = true THEN bw.meeting_2018_goal_oversub
      ELSE FALSE END AS meeting_2018_goal_oversub,
      CASE WHEN fw.fit_for_ia = true THEN bw.ia_bw_mbps_total
      ELSE NULL END AS ia_bw_mbps_total,
      CASE WHEN fw.fit_for_ia = true THEN bw.ia_bandwidth_per_student_kbps
      ELSE NULL END AS ia_bandwidth_per_student_kbps,
      bw.bw_target_status,
      fib.fiber_target_status,
      CASE WHEN ffs.num_470s = 0 Then FALSE
      ELSE TRUE END AS filed_470,
      CASE WHEN ffs.num_fiber_470s = 0 Then FALSE
      ELSE TRUE END AS filed_fiber_470,
      CASE WHEN fw.funding_year = 2019 THEN fw.assumed_unscalable_campuses_fine_wine
          ELSE fw.assumed_unscalable_campuses END  AS assumed_unscalable_campuses,
      CASE WHEN fw.funding_year = 2019 THEN fw.known_unscalable_campuses_fine_wine
          ELSE fw.known_unscalable_campuses END AS known_unscalable_campuses,
      CASE WHEN fw.funding_year = 2019 THEN (fw.assumed_unscalable_campuses_fine_wine + fw.known_unscalable_campuses_fine_wine)
          ELSE (fw.assumed_unscalable_campuses + fw.known_unscalable_campuses) END AS total_unscalable_campuses,
      lydf.assumed_unscalable_campuses AS ly_assumed_unscalable_campuses,
      lydf.known_unscalable_campuses AS ly_known_unscalable_campuses,
      lydf.assumed_unscalable_campuses + lydf.known_unscalable_campuses AS ly_total_unscalable_campuses,
      CASE WHEN nbd.district_id is not null
            THEN TRUE
          ELSE FALSE
      END AS no_bid_district,
      dli.fiber_frns_received_zero_bids,
      CASE WHEN (dli.fiber_frns_received_zero_bids > 0
          OR ffs.num_fiber_470s = 0) THEN true
        ELSE false
      end AS no_fiber_in_area
FROM ps.smd_2019_fine_wine fw

LEFT JOIN ps.districts d
ON fw.district_id = d.district_id
ANd fw.funding_year = d.funding_year

LEFT JOIN ps.districts_fiber fib
ON fw.district_id = fib.district_id
AND fw.funding_year = fib.funding_year

LEFT JOIN ps.districts_bw_cost bw
ON fw.district_id = bw.district_id
AND fw.funding_year = bw.funding_year

LEFT JOIN ps.districts_sp_assignments sp
ON fw.district_id = sp.district_id
AND fw.funding_year = sp.funding_year

LEFT JOIN ps.districts_fit_for_analysis fit
ON fw.district_id = fit.district_id
AND fw.funding_year = fit.funding_year

LEFT JOIN ps.smd_2019_fine_wine lydf
ON fw.district_id = lydf.district_id
AND fw.funding_year = lydf.funding_year + 1

LEFT JOIN ps.districts_470s ffs
ON fw.district_id = ffs.district_id
AND fw.funding_year = ffs.funding_year

LEFT JOIN no_bid_districts nbd
ON fw.district_id = nbd.district_id

LEFT JOIN ps.districts_lines dli
ON fw.district_id = dli.district_id
AND fw.funding_year = dli.funding_year

WHERE fw.district_type = 'Traditional'
),

state_percent AS (
SELECT m.funding_year,
      m.state_code,
      ss.org_structure,
      SUM(m.total_unscalable_campuses)/Sum(m.num_campuses) AS pecent_unscalable_campuses,
      1-(SUM(m.total_unscalable_campuses)/Sum(m.num_campuses)) AS percent_scalable_campuses
FROM metrics m

LEFT JOIN ps.states_static ss
on m.state_code = ss.state_code

GROUP BY m.funding_year,
      m.state_code,
      ss.org_structure)

SELECT m.*,
      stp.pecent_unscalable_campuses,
      stp.percent_scalable_campuses,
      CASE WHEN fiber_target_status = 'Target' THEN
      CASE WHEN state_match  = true then '1 State Match'
          WHEN c1_discount_rate < .5 then '2 Wealthy District'
            WHEN meeting_2018_goal_oversub THEN '3 Meeting 1 Mbps'
            WHEN super_remote_locale = true then '4 Super Remote'
            WHEN meeting_2014_goal_no_oversub THEN '5 Meeting 100 kbps'
            ELSE '6 No Solution'
            END
            ELSE NULL END as fiber_funnel,
      CASE WHEN stp.org_structure = 'state network or state ISP' THEN TRUE
      ELSE false END as state_network,
      CASE WHEN m.total_unscalable_campuses = 0
          AND (m.ly_total_unscalable_campuses) > 0
            THEN TRUE
            ELSE FALSE END AS fiber_upgrade_indicator,
      CASE WHEN m.total_unscalable_campuses = 0
        THEN m.ly_total_unscalable_campuses
        ELSE 0
        END AS campuses_upgraded
FROM metrics m

LEFT JOIN state_percent stp
ON m.state_code = stp.state_code
AND m.funding_year = stp.funding_year


/*WHERE size = 'Mega'
AND m.total_unscalable_campuses = 0
          AND (m.ly_total_unscalable_campuses) > 0
and m.funding_year = 2019*/
