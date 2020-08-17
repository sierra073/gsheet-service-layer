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

    )


SELECT
sbs.state_code,
"Wt Avg $/Student",
"Wt Avg $/Student"/(select "Wt Avg $/Student" from state_by_state where state_code = 'National w/out AK')-1 as "% Diff from National Wt Avg $/Student",
"Median $/Student",
"Wt Avg $/Mbps",
"Wt Avg $/Mbps"/(select "Wt Avg $/Mbps" from state_by_state where state_code = 'National w/out AK')-1 as "% Diff from National Wt Avg $/Mbps",
"% Districts meeting 1Mbps",
state_network

from state_by_state sbs

left join ps.states_static ss
on sbs.state_code = ss.state_code

order BY
  CASE
    WHEN sbs.state_code = 'National'
      then 1
    WHEN sbs.state_code = 'National w/out AK'
      then 2
    WHEN sbs.state_code = 'National Meeting 1Mbps'
      then 3
    ELSE 4
  END
