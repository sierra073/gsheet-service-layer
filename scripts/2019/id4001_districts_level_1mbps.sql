SELECT
      --Demographic
      d.funding_year,
      d.state_code,
      d.district_id,
      d.name,
      d.size,
      d.locale,
      d.num_students,
      d.num_schools,
      ROUND(bw.c1_discount_rate_ia,1) AS c1_discount_rate_ia,

      --bw
      bw.meeting_2014_goal_no_oversub,
      bw.meeting_2018_goal_oversub,
      bw.ia_bandwidth_per_student_kbps,
      bw.knapsack_bandwidth,
      bw.projected_bw_fy2018,
      bw.projected_bw_fy2018_cck12,

      --cost
      bw.ia_monthly_cost_total,
      bw.ia_monthly_cost_per_mbps,
      bw.ia_annual_cost_total,
      bw.meeting_knapsack_affordability_target,
      bw.ia_monthly_cost_total / d.num_students
      AS ia_dollars_per_student,

      --fit
      fit.fit_for_ia,
      fit.fit_for_ia_cost
FROM ps.districts d

JOIN ps.districts_bw_cost bw
ON d.district_id = bw.district_id
AND d.funding_year = bw.funding_year

JOIN ps.districts_fit_for_analysis fit
ON d.district_id = fit.district_id
AND d.funding_year = fit.funding_year


WHERE d.in_universe = true
AND d.district_type = 'Traditional'
