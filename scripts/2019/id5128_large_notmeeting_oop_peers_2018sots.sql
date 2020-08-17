with subset as (
  select
    d.district_id,
    d.state_code,
    ROUND(d.frl_percent,1)as frl_percent,
    d.c1_discount_rate,
    d.num_students,
    dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub,
    dbc.ia_monthly_cost_total as ia_monthly_cost_total,
    dbc.meeting_knapsack_affordability_target as meeting_knapsack_affordability_target,
    dpr.district_id is not null as have_peer_deal,
    dfa.fit_for_ia_cost as fit_for_ia_cost,
    dfa.fit_for_ia as fit_for_ia

  from ps.districts d

  left join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year

  left join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year

  left join (
    select distinct
      funding_year,
      district_id
    from ps.districts_peers_ranks
  ) dpr
  on d.district_id= dpr.district_id
  and d.funding_year = dpr.funding_year

  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
),

categories AS (
  select district_id,
    state_code,
    frl_percent,
    c1_discount_rate,
    num_students,
    meeting_2014_goal_no_oversub,
    ia_monthly_cost_total,
    meeting_knapsack_affordability_target,
    have_peer_deal,
    fit_for_ia_cost,
     fit_for_ia,
    case
            --either have a peer deal to get them what they need or are spending enough to get bw at benchmark affordability
      when  have_peer_deal = true
        then 'budget sufficient at peer deal'
      when (case
              when ia_monthly_cost_total < 14*50 and ia_monthly_cost_total > 0
                then ia_monthly_cost_total/14
              else ps.knapsack_bandwidth(ia_monthly_cost_total)
            end*1000/num_students) >= 100
        then 'budget sufficient at benchmark'
      else 'increase budget'
    end as subgroup
  from subset
  where meeting_2014_goal_no_oversub = false
  and fit_for_ia_cost = true
  and num_students > 9000
),

deal_rank AS (
SELECT c.district_id,
      bcd.ia_monthly_cost_total/c.num_students as district_cost_per_student,
      bcp.district_id as peer_id,
      bcp.ia_monthly_cost_total/dpeer.num_students AS peer_cost_per_student,
      (bcp.ia_annual_cost_total -  bcd.ia_annual_cost_total)/c.num_students
      AS additional_cost_per_student,
      c.num_students as district_num_students,
      dpeer.num_students as peer_num_students,
      bcp.meeting_2014_goal_no_oversub as peer_meeting_2014_goal_no_oversub,
      bcp.ia_monthly_cost_total as peer_monthly_total,
      (bcp.ia_annual_cost_total* (1- dpeer.c1_discount_rate)) as peer_annual_total_out_of_pocket,
      bcd.ia_monthly_cost_total as district_monthly_total,
      (bcd.ia_annual_cost_total* (1- c.c1_discount_rate)) as district_annual_total_out_of_pocket,
      RANK() OVER(PARTITION BY c.district_id
            ORDER BY (bcp.ia_monthly_cost_total/dpeer.num_students) desc) as peer_rank
FROM categories c

LEFT JOIN ps.districts_peers dp
ON c.district_id = dp.district_id
AND  dp.funding_year = 2019

LEFT JOIN ps.districts_bw_cost bcp
ON bcp.district_id = ANY(dp.bandwidth_suggested_districts)
and bcp.funding_year = 2019

LEFT JOIN ps.districts dpeer
on bcp.district_id = dpeer.district_id
and dpeer.funding_year = 2019

LEFT JOIN ps.districts_bw_cost bcd
ON c.district_id = bcd.district_id
and bcd.funding_year = 2019

WHERE c.subgroup = 'increase budget'
)

SELECT case when peer_meeting_2014_goal_no_oversub = true
then 'peers meeting goals'
else 'peers not meeting goals' end as subgroup,
median(peer_annual_total_out_of_pocket/peer_num_students) as median_ia_annual_oop_per_student
FROM deal_rank dr

WHERE dr.peer_rank = 1
group by 1

union

select 'large districts not meeting, need to increase budget' as subgroup,
median(district_annual_total_out_of_pocket/peer_num_students) as median_ia_annual_oop_per_student
FROM deal_rank dr

WHERE dr.peer_rank = 1
group by 1