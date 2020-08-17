with up_districts as (
      with nineteen as ( --districts meeting goal in 2019
      select du.district_id,
      du.funding_year
      from ps.districts_upgrades du
      
      join ps.districts_bw_cost dbc
      on dbc.district_id = du.district_id
      and dbc.funding_year = du.funding_year
      
      join ps.districts_fit_for_analysis dffa
      on dffa.district_id = du.district_id
      and dffa.funding_year = du.funding_year
      
      where (dbc.funding_year = 2019 and dbc.meeting_2018_goal_oversub = true)
      and du.upgrade_indicator = true
      and dffa.fit_for_ia_cost = true

      ),
      eighteen as( --districts NOT meeting goal in 2018
      
      select du.district_id,
      du.funding_year
      from ps.districts_upgrades du
      
      join ps.districts_bw_cost dbc
      on dbc.district_id = du.district_id
      and dbc.funding_year = du.funding_year
      
      join ps.districts_fit_for_analysis dffa
      on dffa.district_id = du.district_id
      and dffa.funding_year = du.funding_year
      
      where (dbc.funding_year = 2018 and dbc.meeting_2018_goal_oversub = false)
      and dffa.fit_for_ia_cost = true
      )
      
      select n.* --consolidate to identify known upgraders
      from nineteen n
      join eighteen e
      on e.district_id=n.district_id
      )
      
      

select ud.district_id,
  d.state_code,
  d.size,
  d.locale,
  d.num_students,
  (select dbc.ia_monthly_cost_total
  from ps.districts_bw_cost dbc
  where dbc.district_id = ud.district_id
  and dbc.funding_year = 2018) as cost_18,
  (select dbc.ia_monthly_cost_total
  from ps.districts_bw_cost dbc
  where dbc.district_id = ud.district_id
  and dbc.funding_year = 2019) as cost_19,
  (select dbc.ia_bandwidth_per_student_kbps
  from ps.districts_bw_cost dbc
  where dbc.district_id = ud.district_id
  and dbc.funding_year = 2019) as bw_per_student,
  
  ( (select dbc.ia_bandwidth_per_student_kbps
  from ps.districts_bw_cost dbc where dbc.district_id = ud.district_id --2019 bwps - 2018 bwps / 2018 bwps
  and dbc.funding_year = 2019) - (select dbc.ia_bandwidth_per_student_kbps
  from ps.districts_bw_cost dbc where dbc.district_id = ud.district_id
  and dbc.funding_year = 2018) ) / (select dbc.ia_bandwidth_per_student_kbps
  from ps.districts_bw_cost dbc where dbc.district_id = ud.district_id
  and dbc.funding_year = 2018) as pct_bwps_increase,
  
  ss.state_network_natl_analysis,
  
  (select dbc.ia_monthly_cost_total
  from ps.districts_bw_cost dbc where dbc.district_id = ud.district_id
  and dbc.funding_year = 2019) -(select dbc.ia_monthly_cost_total
  from ps.districts_bw_cost dbc where dbc.district_id = ud.district_id
  and dbc.funding_year = 2018) as cost_diff, -- 2018 - 2019 total ia cost
  
  ((select dbc.ia_monthly_cost_total
  from ps.districts_bw_cost dbc where dbc.district_id = ud.district_id
  and dbc.funding_year = 2019) - (select dbc.ia_monthly_cost_total
  from ps.districts_bw_cost dbc where dbc.district_id = ud.district_id
  and dbc.funding_year = 2018) )/d.num_students as cost_diff_per_student -- ia cost difference / num_students for per-student cost difference


from up_districts ud

join ps.districts d
on d.district_id = ud.district_id
and d.funding_year = ud.funding_year

join ps.states_static ss
on ss.state_code = d.state_code

join ps.districts_bw_cost dbc
on dbc.funding_year = ud.funding_year
and dbc.district_id = ud.district_id

where dbc.meeting_2018_goal_oversub = true
and d.district_type = 'Traditional'
and d.in_universe = true
