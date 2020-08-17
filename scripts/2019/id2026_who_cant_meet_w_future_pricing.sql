with not_meeting_w_cost as (
          SELECT
          dbc.funding_year,
          dbc.district_id,
          d.num_students,
          d.num_schools,
          d.locale,
          d.size,
          d.c1_discount_rate,
          d.discount_rate_assumed,
          dbc.ia_monthly_cost_total,
          dbc.meeting_2018_goal_oversub,
          dbc.projected_bw_fy2018,
          ceil(round(dbc.projected_bw_fy2018*0.0001,1))*10000 as proj_bw_rounded_up_to_next_10k,
          fit.fit_for_ia_cost

          from ps.districts_bw_cost dbc

          join ps.districts d
          on dbc.district_id = d.district_id
          and dbc.funding_year = d.funding_year

          join ps.districts_fit_for_analysis fit
          on dbc.district_id = fit.district_id
          and dbc.funding_year = fit.funding_year

          where dbc.funding_year = 2019
          and d.in_universe
          and d.district_type = 'Traditional'
      ),

      get_extrap as ( --getting ratio for extrapolation from clean population
          SELECT
          count(district_id)::numeric/count(district_id) filter(where fit_for_ia_cost = true and ia_monthly_cost_total > 0)::numeric as dist_multiplier,
          sum(num_students)::numeric/sum(num_students) filter(where fit_for_ia_cost = true and ia_monthly_cost_total > 0)::numeric as stud_multiplier

          from not_meeting_w_cost

      ),

      state_network_exc as ( -- added to exclude state network states
          SELECT
          district_id,
          ss.state_network_natl_analysis as state_network_tf

          from ps.districts dd

          left join ps.states_static ss
          on dd.state_code = ss.state_code

          where funding_year = 2019
      ),

      needs_1g as (
          SELECT
          'Needs 1G'::text as "Group",
          nm.district_id,
          nm.num_students,
          nm.ia_monthly_cost_total,
          1000-nm.ia_monthly_cost_total as cost_needed,
          CASE
            WHEN nm.discount_rate_assumed = false
              then c1_discount_rate
            ELSE NULL
          END as discount_rate

          from not_meeting_w_cost nm

          join state_network_exc sn
          on nm.district_id = sn.district_id

          where not nm.meeting_2018_goal_oversub
          and nm.fit_for_ia_cost
          and nm.projected_bw_fy2018 <= 1000
          and sn.state_network_tf = false
          and nm.ia_monthly_cost_total < 1000
          and nm.ia_monthly_cost_total > 0
      ),

      needs_10g as (
          SELECT
          'Needs 10G'::text as "Group",
          nm.district_id,
          nm.num_students,
          nm.ia_monthly_cost_total,
          2500-nm.ia_monthly_cost_total as cost_needed,
          CASE
            WHEN nm.discount_rate_assumed = false
              then c1_discount_rate
            ELSE NULL
          END as discount_rate

          from not_meeting_w_cost nm

          join state_network_exc sn
          on nm.district_id = sn.district_id

          where not nm.meeting_2018_goal_oversub
          and nm.fit_for_ia_cost
          and nm.projected_bw_fy2018 > 1000
          and nm.projected_bw_fy2018 <= 10000
          and sn.state_network_tf = false
          and nm.ia_monthly_cost_total < 2500
          and nm.ia_monthly_cost_total > 0
      ),

      needs_more_than_10g as (
          SELECT
          'Needs >10G'::text as "Group",
          nm.district_id,
          nm.num_students,
          nm.ia_monthly_cost_total,
          (proj_bw_rounded_up_to_next_10k*0.25)-nm.ia_monthly_cost_total as cost_needed,
          CASE
            WHEN nm.discount_rate_assumed = false
              then c1_discount_rate
            ELSE NULL
          END as discount_rate

          from not_meeting_w_cost nm

          join state_network_exc sn
          on nm.district_id = sn.district_id

          where not nm.meeting_2018_goal_oversub
          and nm.fit_for_ia_cost
          and nm.projected_bw_fy2018 > 10000
          and sn.state_network_tf = false
          and ia_monthly_cost_total/proj_bw_rounded_up_to_next_10k < 0.25
          and nm.ia_monthly_cost_total > 0
      )

      SELECT
      "Group",
      count(distinct district_id)*(select dist_multiplier from get_extrap) as total_districts,
      sum(num_students)*(select stud_multiplier from get_extrap) as total_students,
      avg(ia_monthly_cost_total) as avg_current_ia_mrc,
      avg(num_students) as avg_num_students,
      AVG(cost_needed) as avg_addtl_mrc,
      AVG(cost_needed/ia_monthly_cost_total) as avg_perc_addtl_mrc,
      median(cost_needed/ia_monthly_cost_total) as median_perc_addtl_mrc,
      avg(cost_needed::numeric/num_students::numeric) as avg_addtl_doll_per_student,
      AVG(discount_rate) as avg_discount_rate

      from needs_1g

      group by "Group"

      UNION

      SELECT
      "Group",
      count(distinct district_id)*(select dist_multiplier from get_extrap) as total_districts,
      sum(num_students)*(select stud_multiplier from get_extrap) as total_students,
      avg(ia_monthly_cost_total) as avg_current_ia_mrc,
      avg(num_students) as avg_num_students,
      AVG(cost_needed) as avg_addtl_mrc,
      AVG(cost_needed/ia_monthly_cost_total) as avg_perc_addtl_mrc,
      median(cost_needed/ia_monthly_cost_total) as median_perc_addtl_mrc,
      avg(cost_needed::numeric/num_students::numeric) as avg_addtl_doll_per_student,
      AVG(discount_rate) as avg_discount_rate

      from needs_10g

      group by "Group"

      UNION

      SELECT
      "Group",
      count(distinct district_id)*(select dist_multiplier from get_extrap) as total_districts,
      sum(num_students)*(select stud_multiplier from get_extrap) as total_students,
      avg(ia_monthly_cost_total) as avg_current_ia_mrc,
      avg(num_students) as avg_num_students,
      AVG(cost_needed) as avg_addtl_mrc,
      AVG(cost_needed/ia_monthly_cost_total) as avg_perc_addtl_mrc,
      median(cost_needed/ia_monthly_cost_total) as median_perc_addtl_mrc,
      avg(cost_needed::numeric/num_students::numeric) as avg_addtl_doll_per_student,
      AVG(discount_rate) as avg_discount_rate

      from needs_more_than_10g

      group by "Group"
