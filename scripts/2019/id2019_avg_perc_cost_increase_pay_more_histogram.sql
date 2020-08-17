SELECT *
FROM
(with pay_more_2018 as (
          SELECT
          d.state_code,
          d.district_id,
          d.name,
          d.locale,
          d.size,
          d.num_students,
          dbc.ia_monthly_cost_total,
          dbc.ia_bw_mbps_total,
          dbc.ia_monthly_cost_per_mbps,
          dbc.ia_bandwidth_per_student_kbps,
          du.path_to_meet_2018_goal_group,
          du.path_to_meet_2018_goal_monthly_cost_needed


          from ps.districts d

          join ps.districts_fit_for_analysis fit
          on d.district_id = fit.district_id
          and d.funding_year = fit.funding_year

          join ps.districts_bw_cost dbc
          on d.district_id = dbc.district_id
          and d.funding_year = dbc.funding_year

          join ps.districts_upgrades du
          on d.district_id = du.district_id
          and d.funding_year = du.funding_year

          where d.funding_year = 2018
          and d.in_universe
          and d.district_type = 'Traditional'
          and fit.fit_for_ia_cost
          and du.path_to_meet_2018_goal_group = 'Pay More'),

      pay_more_status_19 as (
          SELECT
          d.district_id,
          d.locale,
          d.size,
          d.num_students,
          dbc.ia_monthly_cost_total,
          dbc.ia_bw_mbps_total,
          dbc.ia_monthly_cost_per_mbps,
          dbc.ia_bandwidth_per_student_kbps,
          dbc.meeting_2018_goal_oversub,
          CASE
            WHEN du.path_to_meet_2018_goal_group is NULL
            and dbc.meeting_2018_goal_oversub = true
              then 'Newly Meeting'
            ELSE du.path_to_meet_2018_goal_group
          END as funnel_group,
          du.path_to_meet_2018_goal_monthly_cost_needed,
          fit.fit_for_ia_cost

          from pay_more_2018 pm18

          join ps.districts d
          on pm18.district_id = d.district_id
          and d.funding_year = 2019

          join ps.districts_bw_cost dbc
          on pm18.district_id = dbc.district_id
          and dbc.funding_year = 2019

          join ps.districts_fit_for_analysis fit
          on pm18.district_id = fit.district_id
          and fit.funding_year = 2019

          join ps.districts_upgrades du
          on pm18.district_id = du.district_id
          and du.funding_year = 2019
      ),

      pay_more_both_years as (
        SELECT
          pm18.district_id,
          pm18.ia_monthly_cost_total as cost_18,
          pm18.path_to_meet_2018_goal_monthly_cost_needed as pay_more_amount_18,
          pm19.ia_monthly_cost_total as cost_19,
          pm19.path_to_meet_2018_goal_monthly_cost_needed as pay_more_amount_19,
          pm18.path_to_meet_2018_goal_group as funnel_group_18,
          pm19.funnel_group as funnel_group_19,
          pm19.meeting_2018_goal_oversub

        from pay_more_2018 pm18

        join pay_more_status_19 pm19
        on pm18.district_id = pm19.district_id

        where pm19.funnel_group = 'Pay More'
      ),

      newly_meeting as (
        SELECT
        pm18.district_id,
        pm19.locale,
        pm19.size,
        pm18.ia_monthly_cost_total as cost_18,
        pm18.ia_bw_mbps_total as bw_18,
        pm19.ia_monthly_cost_total as cost_19,
        pm19.ia_bw_mbps_total as bw_19,
        pm18.path_to_meet_2018_goal_group as funnel_group_18,
        pm19.funnel_group as funnel_group_19,
        pm19.meeting_2018_goal_oversub

        from pay_more_2018 pm18

        join pay_more_status_19 pm19
        on pm18.district_id = pm19.district_id

        where pm19.funnel_group = 'Newly Meeting'
        and pm19.fit_for_ia_cost
      )

      SELECT
      district_id::varchar,
      locale,
      size,
      (cost_19-cost_18)/cost_18 as perc_cost_increase

      from newly_meeting

      where cost_19 > cost_18
      and bw_19 > bw_18

      UNION

      SELECT
      'Average' as district_id,
      '' as locale,
      '' as size,
      AVG((cost_19-cost_18)/cost_18) as perc_cost_increase

      from newly_meeting

      where cost_19 > cost_18
      and bw_19 > bw_18

      UNION

      SELECT
      'Average w/o outlier' as district_id,
      '' as locale,
      '' as size,
      AVG((cost_19-cost_18)/cost_18) as perc_cost_increase

      from newly_meeting

      where cost_19 > cost_18
      and bw_19 > bw_18
      and district_id != 952343) aa --excluding one outlier district skewing average

ORDER BY
  CASE
    WHEN district_id = 'Average'
      THEN 1
    WHEN district_id = 'Average w/o outlier'
      THEN 2
    ELSE 3
  END
