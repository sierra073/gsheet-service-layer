SELECT *
FROM (
      with not_meeting_w_cost as (
          SELECT
          dbc.district_id,
          d.num_students,
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

      meet_at_10gbps as (
          SELECT
          district_id,
          CASE
            WHEN (10000000/num_students)::numeric/ (CASE
                                                      WHEN size = 'Medium'
                                                        then .85
                                                      WHEN size in ('Large', 'Mega')
                                                        then .7
                                                      ELSE 1
                                                    END) >= 1000
              THEN TRUE
            ELSE FALSE
          END as meets_w_10g

          from not_meeting_w_cost
      ),

      get_extrap as ( --getting ratio for extrapolation from clean population
          SELECT
          count(district_id)::numeric/count(district_id) filter(where fit_for_ia_cost = true)::numeric as dist_multiplier,
          sum(num_students)::numeric/sum(num_students) filter(where fit_for_ia_cost = true)::numeric as stud_multiplier

          from not_meeting_w_cost
      )


      SELECT
        'All Districts' as "Group",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric*(select dist_multiplier from get_extrap) as "Districts above $2500",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric/count(nm.district_id)::numeric as "% Districts above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric*(select stud_multiplier from get_extrap) as "Students above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric/sum(num_students)::numeric as "% Students above $2500"

        from not_meeting_w_cost nm

        left join meet_at_10gbps m10
        on nm.district_id = m10.district_id

        where fit_for_ia_cost
        and meets_w_10g

      UNION

      SELECT
        'Not Meeting 1Mbps' as "Group",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric*(select dist_multiplier from get_extrap) as "Districts above $2500",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric/count(nm.district_id)::numeric as "% Districts above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric*(select stud_multiplier from get_extrap) as "Students above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric/sum(num_students)::numeric as "% Students above $2500"

        from not_meeting_w_cost nm

        left join meet_at_10gbps m10
        on nm.district_id = m10.district_id

        where fit_for_ia_cost
        and not meeting_2018_goal_oversub
        and meets_w_10g

      UNION

      SELECT
        size as "Group",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric*(select dist_multiplier from get_extrap) as "Districts above $2500",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric/count(nm.district_id)::numeric as "% Districts above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric*(select stud_multiplier from get_extrap) as "Students above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric/sum(num_students)::numeric as "% Students above $2500"

        from not_meeting_w_cost nm

        left join meet_at_10gbps m10
        on nm.district_id = m10.district_id

        where fit_for_ia_cost
        and meets_w_10g

        group by size

      UNION

      SELECT
        locale as "Group",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric*(select dist_multiplier from get_extrap) as "Districts above $2500",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric/count(nm.district_id)::numeric as "% Districts above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric*(select stud_multiplier from get_extrap) as "Students above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric/sum(num_students)::numeric as "% Students above $2500"

        from not_meeting_w_cost nm

        left join meet_at_10gbps m10
        on nm.district_id = m10.district_id

        where fit_for_ia_cost
        and meets_w_10g

        group by locale

      UNION

      SELECT
        concat(size,' Not Meeting 1Mbps') as "Group",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric*(select dist_multiplier from get_extrap) as "Districts above $2500",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric/count(nm.district_id)::numeric as "% Districts above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric*(select stud_multiplier from get_extrap) as "Students above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric/sum(num_students)::numeric as "% Students above $2500"

        from not_meeting_w_cost nm

        left join meet_at_10gbps m10
        on nm.district_id = m10.district_id

        where fit_for_ia_cost
        and not meeting_2018_goal_oversub
        and meets_w_10g

        group by size

      UNION

      SELECT
        concat(locale,' Not Meeting 1Mbps') as "Group",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric*(select dist_multiplier from get_extrap) as "Districts above $2500",
        count(nm.district_id) filter(where ia_monthly_cost_total >= 2500)::numeric/count(nm.district_id)::numeric as "% Districts above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric*(select stud_multiplier from get_extrap) as "Students above $2500",
        sum(num_students) filter(where ia_monthly_cost_total >= 2500)::numeric/sum(num_students)::numeric as "% Students above $2500"

        from not_meeting_w_cost nm

        left join meet_at_10gbps m10
        on nm.district_id = m10.district_id

        where fit_for_ia_cost
        and not meeting_2018_goal_oversub
        and meets_w_10g

      group by locale) a

ORDER BY --custom ordering to make easier to follow
  CASE
    WHEN "Group" = 'All Districts'
      then 1
    WHEN "Group" = 'Not Meeting 1Mbps'
      then 2
    WHEN "Group" in ('Rural','Town','Suburban','Urban')
      then 3
    WHEN "Group" in ('Mega','Large','Medium','Small','Tiny')
      then 4
    WHEN "Group" ilike '% Not Meeting%'
      and ("Group" ilike 'Rural' OR
          "Group" ilike 'Town' OR
          "Group" ilike 'Suburban' OR
          "Group" ilike 'Urban')
      then 5
    ELSE 6
  END
