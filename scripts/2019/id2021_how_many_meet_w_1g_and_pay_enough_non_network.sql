SELECT *
FROM
(with not_meeting_w_cost as (
          SELECT
          dbc.funding_year,
          dbc.district_id,
          d.num_students,
          d.num_schools,
          d.locale,
          d.size,
          dbc.ia_monthly_cost_total,
          dbc.meeting_2018_goal_oversub,
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
    --START OF MEGA CONCURRENCY - from ps.districts_bw_cost
    campuses AS(
							SELECT
								cc.funding_year,
								cc.district_id,
								cc.campus_id,
								SUM(ss.num_students) as num_students

							FROM
								dwh.dt_campuses cc

								JOIN dwh.dt_schools ss
								ON cc.campus_id = ss.campus_id
								AND cc.funding_year = ss.funding_year

							WHERE
								ss.closed = FALSE

							GROUP BY
								cc.funding_year,
								cc.district_id,
								cc.campus_id
							),

						mega_campuses AS (
							SELECT
								cc.funding_year,
								dd.district_id,
								cc.campus_id,
								cc.num_students as campus_num_students,
								row_number() over(partition by dd.district_id, dd.funding_year order by cc.num_students desc) as campus_row

							FROM
								campuses cc

								JOIN dwh.dt_districts dd
								ON cc.district_id = dd.district_id
								AND cc.funding_year = dd.funding_year

								JOIN dwh.ft_districts ddn
								ON cc.district_id = ddn.district_id
								AND cc.funding_year = ddn.funding_year

							WHERE
								dd.size = 'Mega'
								AND cc.num_students > 0
								AND ddn.num_schools >= 100
							),

							mega_circuits as (
								SELECT
									dli.funding_year,
									dli.district_id,
									dli.line_item_id,
									dli.num_lines,
									dli.bandwidth_received / dli.num_lines as circuit_received,
									generate_series(1, dli.num_lines)

								FROM
									dwh.ft_districts_line_items dli

									JOIN dwh.dt_line_items li
									ON dli.line_item_id = li.line_item_id
									AND dli.funding_year = li.funding_year

									JOIN dwh.dt_districts dd
									ON dd.district_id = dli.district_id
									AND dd.funding_year = dli.funding_year

									JOIN dwh.ft_districts ddn
									ON dli.district_id = ddn.district_id
									AND dli.funding_year = ddn.funding_year

								WHERE
									ddn.num_schools >= 100
									AND li.purpose = 'wan'
									AND dd.size = 'Mega'
							),

							mega_circuit_ranked AS (
									SELECT
										funding_year,
										district_id,
										line_item_id,
										num_lines,
										circuit_received,
										row_number() over(partition by district_id, funding_year order by circuit_received desc, line_item_id desc) as circuit_row

									FROM
										mega_circuits
							),

							mega_combined AS (
								SELECT
									mc.funding_year,
									mc.district_id,
									mc.campus_id,
									mc.campus_num_students,
									cr.circuit_received,
									CASE
										WHEN cr.circuit_received IS NULL OR cr.circuit_received = 0
											THEN NULL
										ELSE cr.circuit_received / mc.campus_num_students
									END AS campus_bw_per_student

								FROM
									mega_campuses mc

									LEFT JOIN mega_circuit_ranked cr
									ON mc.district_id = cr.district_id
									AND mc.funding_year = cr.funding_year
									AND mc.campus_row = cr.circuit_row
							),

							mega_campus_bw AS (
								SELECT
									funding_year,
									district_id,
									CASE
										WHEN COUNT(campus_id) FILTER (WHERE campus_bw_per_student >= 1) <= 0
											THEN 0
										ELSE ROUND(COUNT(campus_id) FILTER (WHERE campus_bw_per_student >= 1)::numeric
													/
												COUNT(distinct campus_id),2)
									END AS perc_campus_meeting_1mbps

								FROM mega_combined

								GROUP BY
									funding_year,
									district_id
							),
   --END OF MEGA CONCURRENCY

      meet_at_1gbps as (
          SELECT
          nm.funding_year,
          nm.district_id,
          CASE
            WHEN (1000000/num_students)::numeric/ (CASE -- same as in ps.districts_bw_cost
                                                      WHEN size = 'Medium'
                                                        then .85
                                                      WHEN size = 'Large'
                                                        then .7
                                                      WHEN size = 'Mega' AND num_schools < 100
																												THEN .4
																											WHEN size = 'Mega' AND num_schools >= 100 AND mcb.perc_campus_meeting_1mbps >= .9
																												THEN .17
																											WHEN size = 'Mega' AND num_schools >= 100 AND mcb.perc_campus_meeting_1mbps < .9
																												THEN .4
                                                      ELSE 1
                                                    END) >= 1000
              THEN TRUE
            ELSE FALSE
          END as meets_w_1g

          from not_meeting_w_cost nm

          LEFT JOIN mega_campus_bw mcb
						ON nm.district_id = mcb.district_id
						AND nm.funding_year = mcb.funding_year
      ),

      get_extrap as ( --getting ratio for extrapolation from clean population
          SELECT
          count(district_id)::numeric/count(district_id) filter(where fit_for_ia_cost = true)::numeric as dist_multiplier,
          sum(num_students)::numeric/sum(num_students) filter(where fit_for_ia_cost = true)::numeric as stud_multiplier

          from not_meeting_w_cost
      ),

      state_network_exc as ( -- added to exclude state network states
          SELECT
          district_id,
          ss.state_network as state_network_tf

          from ps.districts dd

          left join ps.states_static ss
          on dd.state_code = ss.state_code

          where funding_year = 2019
      )

      SELECT
      '1. All Paying < $2500' as id,
      count(distinct nm.district_id) as sample_districts,
      count(distinct nm.district_id)*(select dist_multiplier from get_extrap) as total_districts,
      sum(num_students) as sample_students,
      sum(num_students)*(select stud_multiplier from get_extrap) as total_students

      from not_meeting_w_cost nm

      left join meet_at_1gbps m1
      on nm.district_id = m1.district_id
      and nm.funding_year = m1.funding_year

      left join state_network_exc sn
      on nm.district_id = sn.district_id

      where nm.ia_monthly_cost_total < 2500
      and fit_for_ia_cost
      and not state_network_tf

      UNION

      SELECT
      '2. Not Meeting 1Mbps' as id,
      count(distinct nm.district_id) as sample_districts,
      count(distinct nm.district_id)*(select dist_multiplier from get_extrap) as total_districts,
      sum(num_students) as sample_students,
      sum(num_students)*(select stud_multiplier from get_extrap) as total_students

      from not_meeting_w_cost nm

      left join meet_at_1gbps m1
      on nm.district_id = m1.district_id
      and nm.funding_year = m1.funding_year

      left join state_network_exc sn
      on nm.district_id = sn.district_id

      where nm.ia_monthly_cost_total < 2500
      and not meeting_2018_goal_oversub
      and fit_for_ia_cost
      and not state_network_tf

      UNION

      SELECT
      '3. Would meet w 1gbps' as id,
      count(distinct nm.district_id) as sample_districts,
      count(distinct nm.district_id)*(select dist_multiplier from get_extrap) as total_districts,
      sum(num_students) as sample_students,
      sum(num_students)*(select stud_multiplier from get_extrap) as total_students

      from not_meeting_w_cost nm

      left join meet_at_1gbps m1
      on nm.district_id = m1.district_id
      and nm.funding_year = m1.funding_year

      left join state_network_exc sn
      on nm.district_id = sn.district_id

      where nm.ia_monthly_cost_total < 2500
      and meets_w_1g
      and fit_for_ia_cost
      and not state_network_tf

      UNION

      SELECT
      '4. Paying > $1000' as id,
      count(distinct nm.district_id) as sample_districts,
      count(distinct nm.district_id)*(select dist_multiplier from get_extrap) as total_districts,
      sum(num_students) as sample_students,
      sum(num_students)*(select stud_multiplier from get_extrap) as total_students

      from not_meeting_w_cost nm

      left join meet_at_1gbps m1
      on nm.district_id = m1.district_id
      and nm.funding_year = m1.funding_year

      left join state_network_exc sn
      on nm.district_id = sn.district_id

      where nm.ia_monthly_cost_total < 2500
      and nm.ia_monthly_cost_total >= 1000
      and fit_for_ia_cost
      and not state_network_tf

      UNION

      SELECT
      '5. Not Meeting, would meet w 1gbps, and paying > $1000' as id,
      count(distinct nm.district_id) as sample_districts,
      count(distinct nm.district_id)*(select dist_multiplier from get_extrap) as total_districts,
      sum(num_students) as sample_students,
      sum(num_students)*(select stud_multiplier from get_extrap) as total_students

      from not_meeting_w_cost nm

      left join meet_at_1gbps m1
      on nm.district_id = m1.district_id
      and nm.funding_year = m1.funding_year

      left join state_network_exc sn
      on nm.district_id = sn.district_id

      where nm.ia_monthly_cost_total < 2500
      and nm.ia_monthly_cost_total >= 1000
      and meets_w_1g
      and fit_for_ia_cost
      and not state_network_tf) t

ORDER BY id
