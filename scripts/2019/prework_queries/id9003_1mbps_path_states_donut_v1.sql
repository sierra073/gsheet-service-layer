with districts_not_meeting as (
select
  d.district_id,
  d.state_code,
  d.num_students,
  dbc.meeting_2018_goal_oversub,
  case when du.path_to_meet_2018_goal_group = 'No Cost Peer Deal' or du.path_to_meet_2018_goal_group = 'New Knapsack Pricing'
    then 'Peer Deal/Affordable'
  when meeting_2018_goal_oversub = true then 'Already Meeting 1 Mbps'
  else du.path_to_meet_2018_goal_group end as path_to_meet_2018_goal_group

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

  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
  and fit.fit_for_ia = true
  and fit.fit_for_ia_cost = true
  and d.state_code != 'DC'
  and ((meeting_2018_goal_oversub = false and path_to_meet_2018_goal_group is not null)
  or (meeting_2018_goal_oversub = true))
),

states_not_meeting_pcts_by_path as (
  select
    dnm.state_code,
    dnm.path_to_meet_2018_goal_group,
    count(dnm.district_id)::numeric/pop.num_districts as pct_districts

  from districts_not_meeting dnm
  join
  (select
    state_code, count(*) as num_districts
   from districts_not_meeting
   group by 1) pop
   on dnm.state_code = pop.state_code

   group by 1,2, pop.num_districts
),

states_not_meeting_pcts_by_path_all_cats as (
select state_code, count(*) as n
from states_not_meeting_pcts_by_path
group by 1
having count(*) = 3),

states_not_meeting_pcts_by_path_l3_cat as (
select state_code, count(*) as n
from states_not_meeting_pcts_by_path
group by 1
having count(*) = 1

union
select state_code, count(*) as n
from states_not_meeting_pcts_by_path
group by 1
having count(*) = 2),

states_categorized_all_cats as (
--# states meeting
  select '1. Nearly all districts meeting 1 Mbps' as category,
  state_code
  from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Already Meeting 1 Mbps'
  and pct_districts >= 0.7
  and state_code in (select state_code from states_not_meeting_pcts_by_path_all_cats)

  union
--# states most have peer deal/affordable
  select '2. Most remaining can meet with peer deal/negotiate' as category,
  pd.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Peer Deal/Affordable') pd
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Pay More') pm
  on pd.state_code = pm.state_code
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Already Meeting 1 Mbps'
  and pct_districts < 0.7) am
  on pd.state_code = am.state_code

  where pd.pct_districts/pm.pct_districts > 1.6
  and pd.state_code in (select state_code from states_not_meeting_pcts_by_path_all_cats)

  union
--# states most need to pay more
  select '4. Most remaining need to pay more' as category,
  pm.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Peer Deal/Affordable') pd
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Pay More') pm
  on pd.state_code = pm.state_code
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Already Meeting 1 Mbps'
  and pct_districts < 0.7) am
  on pd.state_code = am.state_code

  where pm.pct_districts/pd.pct_districts > 1.6
  and pm.state_code in (select state_code from states_not_meeting_pcts_by_path_all_cats)

  union
--# states most split
  select '3. Most remaining split' as category,
  pm.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Peer Deal/Affordable') pd
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Pay More') pm
  on pd.state_code = pm.state_code
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Already Meeting 1 Mbps'
  and pct_districts < 0.7) am
  on pd.state_code = am.state_code

  where not(pd.pct_districts/pm.pct_districts > 1.6 or pm.pct_districts/pd.pct_districts > 1.6)
  and pm.state_code in (select state_code from states_not_meeting_pcts_by_path_all_cats)

),

states_categorized_l3_cat as (
  --# states meeting
    select '1. Nearly all districts meeting 1 Mbps' as category,
    state_code
    from states_not_meeting_pcts_by_path
    where path_to_meet_2018_goal_group = 'Already Meeting 1 Mbps'
    and pct_districts >= 0.7
    and state_code in (select state_code from states_not_meeting_pcts_by_path_l3_cat)

  union
--# states most have peer deal/affordable
  select '2. Most remaining can meet with peer deal/negotiate' as category,
  pd.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Peer Deal/Affordable') pd

  where pd.state_code in (select state_code from states_not_meeting_pcts_by_path_l3_cat where n = 1)

  union
--# states most have peer deal/affordable
  select '2. Most remaining can meet with peer deal/negotiate' as category,
  pd.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Peer Deal/Affordable') pd
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Already Meeting 1 Mbps'
  and pct_districts < 0.7) am
  on pd.state_code = am.state_code

  where pd.state_code in (select state_code from states_not_meeting_pcts_by_path_l3_cat where n = 2)

  union
--# states most need to pay more
  select '4. Most remaining need to pay more' as category,
  pm.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Pay More') pm

  where pm.state_code in (select state_code from states_not_meeting_pcts_by_path_l3_cat where n = 1)

  union
--# states most need to pay more
  select '4. Most remaining need to pay more' as category,
  pm.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Pay More') pm
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Already Meeting 1 Mbps'
  and pct_districts < 0.7) am
  on pm.state_code = am.state_code

  where pm.state_code in (select state_code from states_not_meeting_pcts_by_path_l3_cat where n = 2)

  union
--# states most have peer deal/affordable
select '2. Most remaining can meet with peer deal/negotiate' as category,
  pd.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Peer Deal/Affordable') pd
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Pay More') pm
  on pd.state_code = pm.state_code

  where pd.pct_districts/pm.pct_districts > 1.6
  and pd.state_code in (select state_code from states_not_meeting_pcts_by_path_l3_cat where n = 2)

  union
--# states most need to pay more
select '4. Most remaining need to pay more' as category,
  pd.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Peer Deal/Affordable') pd
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Pay More') pm
  on pd.state_code = pm.state_code

  where pm.pct_districts/pd.pct_districts > 1.6
  and pd.state_code in (select state_code from states_not_meeting_pcts_by_path_l3_cat where n = 2)

union
--# states most split
  select '3. Most remaining split' as category,
  pd.state_code
  from
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Peer Deal/Affordable') pd
  join
  (select * from states_not_meeting_pcts_by_path
  where path_to_meet_2018_goal_group = 'Pay More') pm
  on pd.state_code = pm.state_code

  where not(pd.pct_districts/pm.pct_districts > 1.6 or pm.pct_districts/pd.pct_districts > 1.6)
  and pd.state_code in (select state_code from states_not_meeting_pcts_by_path_l3_cat where n = 2)
),

states_categorized as (
select * from states_categorized_all_cats
union
select * from states_categorized_l3_cat
order by 1)

select * from states_categorized
