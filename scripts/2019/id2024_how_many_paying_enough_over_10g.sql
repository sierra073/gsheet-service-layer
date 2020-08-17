select *

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
          count(district_id)::numeric/count(district_id) filter(where fit_for_ia_cost = true)::numeric as dist_multiplier,
          sum(num_students)::numeric/sum(num_students) filter(where fit_for_ia_cost = true)::numeric as stud_multiplier

          from not_meeting_w_cost

          where num_students > 10000 --extrapolating based on the population of large districts for precision (they are likely more clean than the overall)
      ),

      state_network_exc as ( -- added to exclude state network states
          SELECT
          district_id,
          ss.state_network_natl_analysis as state_network_tf

          from ps.districts dd

          left join ps.states_static ss
          on dd.state_code = ss.state_code

          where funding_year = 2019
      )

SELECT
'1. Dont meet at 10g and not SN' as "Group",
count(distinct nm.district_id) as sample_districts,
count(distinct nm.district_id)*(select dist_multiplier from get_extrap) as total_districts,
sum(nm.num_students) as sample_students,
sum(nm.num_students)*(select stud_multiplier from get_extrap) as total_students

from not_meeting_w_cost nm

join state_network_exc sn
on nm.district_id = sn.district_id

where not nm.meeting_2018_goal_oversub
and nm.fit_for_ia_cost
and nm.projected_bw_fy2018 > 10000
and sn.state_network_tf = false

UNION

SELECT
'2. Can meet w $0.25/mbps' as "Group",
count(distinct nm.district_id) as sample_districts,
count(distinct nm.district_id)*(select dist_multiplier from get_extrap) as total_districts,
sum(nm.num_students) as sample_students,
sum(nm.num_students)*(select stud_multiplier from get_extrap) as total_students

from not_meeting_w_cost nm

join state_network_exc sn
on nm.district_id = sn.district_id

where not nm.meeting_2018_goal_oversub
and nm.fit_for_ia_cost
and nm.projected_bw_fy2018 > 10000
and sn.state_network_tf = false
and ia_monthly_cost_total/projected_bw_fy2018 >= 0.25

UNION

SELECT
'3. If only buy in 10k increments' as "Group",
count(distinct nm.district_id) as sample_districts,
count(distinct nm.district_id)*(select dist_multiplier from get_extrap) as total_districts,
sum(nm.num_students) as sample_students,
sum(nm.num_students)*(select stud_multiplier from get_extrap) as total_students

from not_meeting_w_cost nm

join state_network_exc sn
on nm.district_id = sn.district_id

where not nm.meeting_2018_goal_oversub
and nm.fit_for_ia_cost
and nm.projected_bw_fy2018 > 10000
and sn.state_network_tf = false
and ia_monthly_cost_total/proj_bw_rounded_up_to_next_10k >= 0.25) tt

order by "Group"
