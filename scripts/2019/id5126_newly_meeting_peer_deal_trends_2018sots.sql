with subset as (
select 
  dd.district_id,
  dd.name,
  dd.state_code,
  dd.num_students,
  dd.locale,
  dd.size,
  dd.frl_percent,
  dfa2.fit_for_ia,
  dfa2.fit_for_ia_cost,
  dbc2.meeting_2014_goal_no_oversub,
  dfa.fit_for_ia as fit_for_ia_18,
  dfa.fit_for_ia_cost as fit_for_ia_cost_18,
  dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_18,
  du.switcher,
  sum(case when dpr.peer_id is not null then 1 else 0 end) > 0 as has_peer_deal
  
from ps.districts dd

join ps.districts_bw_cost dbc
on dd.district_id= dbc.district_id
and dd.funding_year = dbc.funding_year

join ps.districts_bw_cost dbc2
on dd.district_id= dbc2.district_id
and dd.funding_year = dbc2.funding_year + 1

join ps.districts_fit_for_analysis dfa
on dd.district_id= dfa.district_id
and dd.funding_year = dfa.funding_year

join ps.districts_fit_for_analysis dfa2
on dd.district_id= dfa2.district_id
and dd.funding_year = dfa2.funding_year + 1

join ps.districts_upgrades  du
on dd.district_id= du.district_id
and dd.funding_year = du.funding_year 

left join ps.districts_peers_ranks dpr
on dd.district_id= dpr.district_id
and dd.funding_year = dpr.funding_year 

where dd.funding_year = 2019
and dd.in_universe = true
and dd.district_type = 'Traditional'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14),

not_meeting_lrg_only_peer_deal as (
select 
      sum(num_students) as num_students,
      count(*) as num_districts
      from subset
      where fit_for_ia_18 = true
      and fit_for_ia_cost_18 = true
      and num_students > 9000
      and has_peer_deal = true
      and meeting_2014_goal_no_oversub_18 = false
),

final_newly_meeting_sample as (
select 
  district_id, num_students, name, state_code, locale, size, frl_percent,
  meeting_2014_goal_no_oversub_18,
  switcher
  
  from subset
  
  where fit_for_ia = true
  and meeting_2014_goal_no_oversub = false
  and fit_for_ia_18 = true
)

------switcher % districts and students of last year cohort (newly meeting), 
------projected onto this year's not meeting large districts with a peer deal
select 
case when switcher = true then 'switcher' else 'non-switcher' end as dimension,
count(district_id)::numeric/
(select count(district_id) as nd from final_newly_meeting_sample where meeting_2014_goal_no_oversub_18 = true) as pct_districts,
sum(final_sample.num_students)::numeric/
(select sum(num_students) as ns from final_newly_meeting_sample where meeting_2014_goal_no_oversub_18 = true) as pct_students,

sum(final_sample.num_students)::numeric/
(select sum(num_students) as ns from final_newly_meeting_sample where meeting_2014_goal_no_oversub_18 = true) * 
not_meeting_lrg_only_peer_deal.num_students as num_students_extrap

from final_newly_meeting_sample as final_sample
join not_meeting_lrg_only_peer_deal
on true
where final_sample.meeting_2014_goal_no_oversub_18 = true
and final_sample.switcher is not null
group by 1, not_meeting_lrg_only_peer_deal.num_students