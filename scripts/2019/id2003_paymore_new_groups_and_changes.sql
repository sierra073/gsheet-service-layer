with pay_more_2018 as (
    SELECT
    d.state_code,
    d.district_id,
    d.name,
    d.num_students,
    dbc.ia_monthly_cost_total,
    dbc.ia_bw_mbps_total,
    dbc.ia_monthly_cost_per_mbps,
    dbc.ia_bandwidth_per_student_kbps,
    du.path_to_meet_2018_goal_group


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

pay_more_movers as (
    SELECT
    pm18.state_code,
    pm18.district_id,
    pm18.name,
    pm18.num_students as num_students_18,
    d.num_students as num_students_19,
    fit.fit_for_ia_cost,
    pm18.ia_monthly_cost_total as total_monthly_cost_18,
    dbc.ia_monthly_cost_total as total_monthly_cost_19,
    pm18.ia_monthly_cost_per_mbps as cost_per_mbps_18,
    dbc.ia_monthly_cost_per_mbps as cost_per_mbps_19,
    pm18.ia_bw_mbps_total as ia_bw_mbps_total_18,
    dbc.ia_bw_mbps_total as ia_bw_mbps_total_19,
    pm18.ia_bandwidth_per_student_kbps as kbps_per_student_2018,
    dbc.ia_bandwidth_per_student_kbps as kbps_per_student_2019,
    dbc.meeting_2018_goal_oversub,
    pm18.path_to_meet_2018_goal_group as "FY18 Group",
    du.path_to_meet_2018_goal_group as "FY19 Group"

    from pay_more_2018 pm18

    left join ps.districts_upgrades du
    on pm18.district_id = du.district_id

    left join ps.districts_bw_cost dbc
    on du.district_id = dbc.district_id
    and du.funding_year = dbc.funding_year

    left join ps.districts_fit_for_analysis fit
    on du.district_id = fit.district_id
    and du.funding_year = fit.funding_year

    left join ps.districts d
    on du.district_id = d.district_id
    and du.funding_year = d.funding_year

    where du.funding_year = 2019
    and ((du.path_to_meet_2018_goal_group != 'Pay More' and du.path_to_meet_2018_goal_group != 'New Knapsack Pricing')
          or (du.path_to_meet_2018_goal_group is null and dbc.meeting_2018_goal_oversub = true))
    and fit_for_ia_cost)


SELECT distinct
  case
    when "FY19 Group" is null
      then 'Newly Meeting'
    else "FY19 Group"
  end as "New Group",
count(distinct district_id) as "Total Districts (pre-extrap)",
count(distinct case
                when ia_bw_mbps_total_18 < ia_bw_mbps_total_19
                  then district_id end)::numeric/count(distinct district_id)::numeric as "% Districts with BW increase",
count(distinct case
                when (total_monthly_cost_18 > total_monthly_cost_19
                and ia_bw_mbps_total_18 < ia_bw_mbps_total_19)
                  then district_id end)::numeric/count(distinct case
                                                                  when ia_bw_mbps_total_18 < ia_bw_mbps_total_19
                                                                    then district_id end)::numeric as "% BW Increase Districts that pay less now",
(sum(ia_bw_mbps_total_19-ia_bw_mbps_total_18) filter(where ia_bw_mbps_total_18 < ia_bw_mbps_total_19))::numeric/(sum(ia_bw_mbps_total_18) filter(where ia_bw_mbps_total_18 < ia_bw_mbps_total_19))::numeric as "Avg % Change BW",
(sum(cost_per_mbps_19-cost_per_mbps_18) filter(where ia_bw_mbps_total_18 < ia_bw_mbps_total_19))::numeric/(sum(cost_per_mbps_18) filter(where ia_bw_mbps_total_18 < ia_bw_mbps_total_19))::numeric as "Avg % Change Cost/Mbps",
count(distinct case
                when total_monthly_cost_18 < total_monthly_cost_19
                  then district_id end)::numeric/count(distinct district_id)::numeric as "% of Districts with cost increase"

from pay_more_movers

group by "New Group"
