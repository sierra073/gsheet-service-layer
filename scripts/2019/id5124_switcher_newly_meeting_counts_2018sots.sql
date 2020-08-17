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

newly_meeting as (
  select 
    round(sample_groups.num_districts*sample_pop.population_districts::numeric/sample_pop.sample_districts,0)- 
       meeting_2018_pop.districts_meeting_100kbps 
        as num_districts,
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,0)- 
       meeting_2018_pop.students_meeting_100kbps 
        as num_students
    from (
      select 
        meeting_2014_goal_no_oversub,
        count(*) as num_districts,
        sum(num_students) as num_students
      from subset
      where fit_for_ia = true
      group by 1
    ) sample_groups
    join (
      select 
        count(*) as population_districts,
        count(*) FILTER (WHERE fit_for_ia = true) as sample_districts,
        sum(num_students) as population_students,
        sum(num_students) FILTER (WHERE fit_for_ia = true) as sample_students
      from subset
    ) sample_pop  
    on true
    left join (
      select districts_meeting_100kbps,
      students_meeting_100kbps
      from ps.state_snapshot_frozen_sots
      where funding_year = 2018
      and state_code = 'ALL'
    ) meeting_2018_pop
    on true
    where sample_groups.meeting_2014_goal_no_oversub = true
),

final_sample as (
select 
  district_id, name, state_code, num_students, locale, size, frl_percent,
  case when hierarchy_ia_connect_category_py != hierarchy_ia_connect_category
  then true
  else false end as changed_technology,
  switcher
  
  from subset
  
  where meeting_2014_goal_no_oversub_py = false
  and meeting_2014_goal_no_oversub = true
  and fit_for_ia = true
  and fit_for_ia_py = true
  and switcher is not null
)

select fs.switcher,
round((count(fs.district_id)::numeric/
  (select count(*) as sample_pop from final_sample)) *
  --population newly meeting
  (select num_districts from newly_meeting)::numeric,0) as num_districts

from final_sample fs
group by 1