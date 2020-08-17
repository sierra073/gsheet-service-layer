with core as (

with cost_calc as (

with base as (
  select
  distinct dli.district_id,
  d.num_students,
  d.size,
  ss.state_code,
  dbc.meeting_2018_goal_oversub,
  dbc.ia_bandwidth_per_student_kbps,

  CASE
    when ((d.num_students * 1000) / 10000000)::int >= 1 --integer of 10G lines needed to reach goal bw
      then ((d.num_students * 1000) / 10000000)::int
      else 0
    end as num_10g_needed,
  
  CASE
    when ( ( (d.num_students * 1000) % 10000000) / 1000000)::int >=1 --remainder aftr 10G lines counted
      then ( ( (d.num_students * 1000) % 10000000) / 1000000)::int + 1
      else 0
      end as num_1g_needed --remainder mod-divided by 1G

  from
  ps.districts_line_items dli
  
  JOIN ps.districts d 
  on d.district_id = dli.district_id
  and d.funding_year = dli.funding_year
  
  join ps.districts_bw_cost dbc
  on dbc.district_id = dli.district_id
  and dbc.funding_year = dli.funding_year
  
  join ps.states_static ss 
  on d.state_code = ss.state_code
  
  join ps.line_items li 
  on dli.line_item_id = li.line_item_id
  and dli.funding_year = li.funding_year
  
  where
  d.funding_year = 2019
  and d.district_type = 'Traditional'
  and d.in_universe = true
  and ss.state_network = true
  and li.connect_category in ('Lit Fiber','Fiber')

)
--assume 2020 pricing based on Kat's projections (1G = $1158, 10G = $2433)
select b.district_id,
  b.num_students,
  b.size,
  b.state_code,
  b.meeting_2018_goal_oversub,
  b.ia_bandwidth_per_student_kbps,
  case when b.num_1g_needed > 2 --bc it's cheaper to get a 10G than 3 or more 1Gs, assume districts opt for more scale
    then (b.num_10g_needed + 1)
    else b.num_10g_needed 
  end as num_10g_needed,
  case when b.num_1g_needed >2
  then 0 
  else b.num_1g_needed
    end as num_1g_needed


from base b

order by b.state_code asc

)

select cc.*,
(1158 * num_1g_needed) + (2433 * num_10g_needed) as est_cost --awaiting state-by-state projections

from cost_calc cc

)

select c.state_code,
sum(c.num_students) AS stud_pop,
count(distinct c.district_id) as district_count,
sum(c.num_10g_needed) as tengig_lines,
sum(c.num_1g_needed) as onegig_lines,
  sum(c.est_cost) as tot_cost
  
from core c

group by c.state_code

order by c.state_code asc