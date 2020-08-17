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
  df.hierarchy_ia_connect_category,
  df2.hierarchy_ia_connect_category as hierarchy_ia_connect_category_py,
  dbc.meeting_2014_goal_no_oversub,
  dbc2.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_py,
  du.switcher
  
from ps.districts d

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_bw_cost dbc2
on d.district_id= dbc2.district_id
and d.funding_year = dbc2.funding_year + 1

join ps.districts_fiber df
on d.district_id= df.district_id
and d.funding_year = df.funding_year

join ps.districts_fiber df2
on d.district_id= df2.district_id
and d.funding_year = df2.funding_year + 1

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
  district_id, name, state_code, num_students, locale, size, frl_percent,
  hierarchy_ia_connect_category,hierarchy_ia_connect_category_py,
  switcher
  
  from subset
  
  where meeting_2014_goal_no_oversub_py = false
  and meeting_2014_goal_no_oversub = true
  and fit_for_ia = true
  and fit_for_ia_py = true
  and switcher is not null
)

-------locale
select 'switcher' as switcher,
case when locale in ('Rural','Town') then '1. Rural/Town'
else '1. Suburban/Urban' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where switcher = true) as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where switcher = true) as pct_students

from final_sample
where switcher = true
group by 1,2
union
select 'not_switcher' as switcher,
case when locale in ('Rural','Town') then '1. Rural/Town'
else '1. Suburban/Urban' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where switcher = false) as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where switcher = false) as pct_students

from final_sample
where switcher = false
group by 1,2

union
------size
select 'switcher' as switcher,
case when size in ('Tiny','Small') then '2. Tiny/Small'
when size in ('Large','Mega') then '2. Large/Mega' 
else '2. Medium' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where switcher = true) as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where switcher = true) as pct_students

from final_sample
where switcher = true
group by 1,2
union
select 'not_switcher' as switcher,
case when size in ('Tiny','Small') then '2. Tiny/Small'
when size in ('Large','Mega') then '2. Large/Mega' 
else '2. Medium' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where switcher = false) as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where switcher = false) as pct_students

from final_sample
where switcher = false
group by 1,2

union

------frl
select 'switcher' as switcher,
case when frl_percent < .75 then '3. < 75% FRL'
else '3. >= 75% FRL' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where switcher = true) as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where switcher = true) as pct_students

from final_sample
where switcher = true
group by 1,2
union
select 'not_switcher' as switcher,
case when frl_percent < .75 then '3. < 75% FRL'
else '3. >= 75% FRL' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where switcher = false) as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where switcher = false) as pct_students

from final_sample
where switcher = false
group by 1,2

union
--technology change
select 'switcher' as switcher,
case when hierarchy_ia_connect_category != hierarchy_ia_connect_category_py
then '4. changed technology'
else '4. didnt change technology' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where switcher = true) as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where switcher = true) as pct_students

from final_sample
where switcher = true
group by 1,2
union
select 'not_switcher' as switcher,
case when hierarchy_ia_connect_category != hierarchy_ia_connect_category_py
then '4. changed technology'
else '4. didnt change technology' end as dimension,
count(district_id)::numeric/
(select count(district_id) from final_sample where switcher = false) as pct_districts,
sum(num_students)::numeric/
(select sum(num_students) from final_sample where switcher = false) as pct_students

from final_sample
where switcher = false
group by 1,2
order by 1,2