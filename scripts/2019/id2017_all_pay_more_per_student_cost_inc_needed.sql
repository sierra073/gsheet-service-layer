with pay_more_2018 as (
          SELECT
          d.funding_year,
          d.state_code,
          d.district_id,
          d.name,
          d.locale,
          d.size,
          d.num_students,
          dbc.c1_discount_rate_ia,
          dbc.ia_monthly_cost_total,
          dbc.ia_bw_mbps_total,
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

      pay_more_2019 as (
          SELECT
          d.funding_year,
          d.state_code,
          d.district_id,
          d.name,
          d.locale,
          d.size,
          d.num_students,
          dbc.c1_discount_rate_ia,
          dbc.ia_monthly_cost_total,
          dbc.ia_bw_mbps_total,
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

          where d.funding_year = 2019
          and d.in_universe
          and d.district_type = 'Traditional'
          and fit.fit_for_ia_cost
          and du.path_to_meet_2018_goal_group = 'Pay More')

      SELECT
      funding_year,
      count(distinct district_id) as sample_count,
      median(path_to_meet_2018_goal_monthly_cost_needed/num_students) as median_cost_inc_needed_per_student,
      median((path_to_meet_2018_goal_monthly_cost_needed/num_students)*(1-c1_discount_rate_ia)) as median_oop_cost_inc_needed_per_student

      from pay_more_2018

      group by funding_year

      UNION

      SELECT
      funding_year,
      count(distinct district_id) as sample_count,
      median(path_to_meet_2018_goal_monthly_cost_needed/num_students) as median_cost_inc_needed_per_student,
      median((path_to_meet_2018_goal_monthly_cost_needed/num_students)*(1-c1_discount_rate_ia)) as median_oop_cost_inc_needed_per_student

      from pay_more_2019

      group by funding_year
