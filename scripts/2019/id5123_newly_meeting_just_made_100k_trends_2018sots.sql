with subset as (
select 
  d.district_id,
  d.name,
  d.state_code,
  d.num_students,
  d.locale,
  d.size,
  d.frl_percent,
  dfa.fit_for_ia,
  dfa2.fit_for_ia as fit_for_ia_py,
  dbc.meeting_2014_goal_no_oversub,
  dbc2.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_py,
  du.switcher,
  dbc.ia_bandwidth_per_student_kbps,
  dbc.ia_monthly_cost_total,
  dbc2.ia_monthly_cost_total as ia_monthly_cost_total_py,
  most_recent_ia_contract_end_date
  
from ps.districts d

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_bw_cost dbc2
on d.district_id= dbc2.district_id
and d.funding_year = dbc2.funding_year + 1

join ps.districts_lines dl2
on d.district_id= dl2.district_id
and d.funding_year = dl2.funding_year + 1

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

join ps.districts_fit_for_analysis dfa2
on d.district_id= dfa2.district_id
and d.funding_year = dfa2.funding_year + 1

join ps.districts_upgrades  du
on d.district_id= du.district_id
and d.funding_year = du.funding_year 

where d.funding_year = 2019
and d.in_universe = true
and d.district_type = 'Traditional'
order by 1),


final_sample as (
select 
  subset.*,
  case when ia_bandwidth_per_student_kbps <= 150 then 'just made to 100 kbps'
  else 'bigger upgrade' end as newly_meeting_bw
  
  from subset
  
  where meeting_2014_goal_no_oversub_py = false
  and meeting_2014_goal_no_oversub = true
  and fit_for_ia = true
  and fit_for_ia_py = true
  and switcher is not null
)

------switcher
select newly_meeting_bw,
case when switcher = true then '2. switcher' else '2. non-switcher' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw = 'just made to 100 kbps'
group by 1,2
union
select newly_meeting_bw,
case when switcher = true then '2. switcher' else '2. non-switcher' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw != 'just made to 100 kbps'

group by 1,2

union
------frl
select newly_meeting_bw,
case when frl_percent < .75 then '1. < 75% FRL'
else '1. >= 75% FRL' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw = 'just made to 100 kbps'
group by 1,2
union
select newly_meeting_bw,
case when frl_percent < .75 then '1. < 75% FRL'
else '1. >= 75% FRL' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw != 'just made to 100 kbps'
group by 1,2

union
------locale
select newly_meeting_bw,
case when locale in ('Rural','Town') then '3. Rural/Town'
else '3. Suburban/Urban' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw = 'just made to 100 kbps'
group by 1,2
union
select newly_meeting_bw,
case when locale in ('Rural','Town') then '3. Rural/Town'
else '3. Suburban/Urban' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw != 'just made to 100 kbps'
group by 1,2

union
------size
select newly_meeting_bw,
case when size in ('Tiny','Small') then '4. Tiny/Small'
when size in ('Large','Mega') then '4. Large/Mega' 
else '4. Medium' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw = 'just made to 100 kbps'
group by 1,2
union
select newly_meeting_bw,
case when size in ('Tiny','Small') then '4. Tiny/Small'
when size in ('Large','Mega') then '4. Large/Mega' 
else '4. Medium' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw != 'just made to 100 kbps'

group by 1,2

union
-----no increase in total cost
select newly_meeting_bw,
case when ia_monthly_cost_total <= ia_monthly_cost_total_py then '5. saw no increase in cost'
else '5. saw increase in cost' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw = 'just made to 100 kbps'
group by 1,2
union
select newly_meeting_bw,
case when ia_monthly_cost_total <= ia_monthly_cost_total_py then '5. saw no increase in cost'
else '5. saw increase in cost' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw != 'just made to 100 kbps'

group by 1,2

union
-----contract expiration
select newly_meeting_bw,
case when most_recent_ia_contract_end_date::date <= '2019-08-31' then '6. contract expiring 2019'
else '6. no expiring contract' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw = 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw = 'just made to 100 kbps'
group by 1,2
union
select newly_meeting_bw,
case when most_recent_ia_contract_end_date::date <= '2019-08-31' then '6. contract expiring 2019'
else '6. no expiring contract' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where newly_meeting_bw != 'just made to 100 kbps') as pct_students

from final_sample
where newly_meeting_bw != 'just made to 100 kbps'

group by 1,2
order by 1,2