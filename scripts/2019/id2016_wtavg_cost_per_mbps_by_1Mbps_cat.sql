with district_spending as (
        select
        d.state_code,
        d.district_id,
        d.num_students,
        ia_monthly_cost_total::numeric/num_students::numeric as raw_dollars_per_student,
        ia_monthly_cost_per_mbps,
        ia_monthly_cost_total,
        ia_bw_mbps_total,
        meeting_2018_goal_oversub

        from ps.districts d

        left join ps.districts_bw_cost dbc
        on d.district_id = dbc.district_id
        and d.funding_year = dbc.funding_year

        left join ps.districts_fit_for_analysis fit
        on d.district_id = fit.district_id
        and d.funding_year = fit.funding_year

        where d.funding_year = 2019
        and in_universe
        and district_type = 'Traditional'
        and fit_for_ia_cost
    ),

    state_by_state as (
        SELECT
        state_code,
        sum(ia_monthly_cost_total)::numeric/sum(num_students)::numeric as "Wt Avg $/Student",
        median(raw_dollars_per_student) as "Median $/Student",
        sum(ia_monthly_cost_total)::numeric/sum(ia_bw_mbps_total)::numeric as "Wt Avg $/Mbps",
        count(distinct district_id) filter(where meeting_2018_goal_oversub = true)::numeric/count(distinct district_id)::numeric as "% Districts meeting 1Mbps"

        from district_spending

        where state_code != 'DC'

        group by state_code

        UNION

        SELECT
        'National' as state_code,
        sum(ia_monthly_cost_total)::numeric/sum(num_students)::numeric as "Wt Avg $/Student",
        median(raw_dollars_per_student) as "Median $/Student",
        sum(ia_monthly_cost_total)::numeric/sum(ia_bw_mbps_total)::numeric as "Wt Avg $/Mbps",
        count(distinct district_id) filter(where meeting_2018_goal_oversub = true)::numeric/count(distinct district_id)::numeric as "% Districts meeting 1Mbps"

        from district_spending

        where state_code != 'DC'

        UNION

        SELECT
        'National w/out AK' as state_code,
        sum(ia_monthly_cost_total)::numeric/sum(num_students)::numeric as "Wt Avg $/Student",
        median(raw_dollars_per_student) as "Median $/Student",
        sum(ia_monthly_cost_total)::numeric/sum(ia_bw_mbps_total)::numeric as "Wt Avg $/Mbps",
        count(distinct district_id) filter(where meeting_2018_goal_oversub = true)::numeric/count(distinct district_id)::numeric as "% Districts meeting 1Mbps"

        from district_spending

        where state_code != 'DC'
        and state_code != 'AK'

        UNION

        SELECT
        'National Meeting 1Mbps' as state_code,
        sum(ia_monthly_cost_total)::numeric/sum(num_students)::numeric as "Wt Avg $/Student",
        median(raw_dollars_per_student) as "Median $/Student",
        sum(ia_monthly_cost_total)::numeric/sum(ia_bw_mbps_total)::numeric as "Wt Avg $/Mbps",
        count(distinct district_id) filter(where meeting_2018_goal_oversub = true)::numeric/count(distinct district_id)::numeric as "% Districts meeting 1Mbps"

        from district_spending

        where state_code != 'DC'
        and meeting_2018_goal_oversub
    ),

  new_groups as (
      SELECT
      '1Mbps Leaders' as category,
      sum(ia_monthly_cost_total)::numeric/sum(ia_bw_mbps_total)::numeric as wt_avg_cost_mbps

      from district_spending ds

      inner join state_by_state sbs
      on ds.state_code = sbs.state_code

      inner join ps.states_static ss
      on ds.state_code = ss.state_code

      where "% Districts meeting 1Mbps" >= 0.5
      and ss.state_network = true

      UNION

      SELECT
      'Middle of the Pack' as category,
      sum(ia_monthly_cost_total)::numeric/sum(ia_bw_mbps_total)::numeric as wt_avg_cost_mbps

      from district_spending ds

      inner join state_by_state sbs
      on ds.state_code = sbs.state_code

      inner join ps.states_static ss
      on ds.state_code = ss.state_code

      where "% Districts meeting 1Mbps" < 0.5
      and "% Districts meeting 1Mbps" >= 0.2
      and ss.state_network = true

      UNION

      SELECT
      '1Mbps Laggers' as category,
      sum(ia_monthly_cost_total)::numeric/sum(ia_bw_mbps_total)::numeric as wt_avg_cost_mbps

      from district_spending ds

      inner join state_by_state sbs
      on ds.state_code = sbs.state_code

      inner join ps.states_static ss
      on ds.state_code = ss.state_code

      where "% Districts meeting 1Mbps" < 0.2
      and ss.state_network = true

      UNION

      SELECT
      'All Except Leaders' as category,
      sum(ia_monthly_cost_total)::numeric/sum(ia_bw_mbps_total)::numeric as wt_avg_cost_mbps

      from district_spending ds

      inner join state_by_state sbs
      on ds.state_code = sbs.state_code

      inner join ps.states_static ss
      on ds.state_code = ss.state_code

      where "% Districts meeting 1Mbps" < 0.5
      and ss.state_network = true)

  SELECT *

  from new_groups

  ORDER BY
    CASE
      WHEN category = '1Mbps Leaders'
        then 1
      WHEN category = 'Middle of the Pack'
        then 2
      WHEN category = '1Mbps Laggers'
        then 3
      WHEN category = 'All Except Leaders'
        then 4
    END
