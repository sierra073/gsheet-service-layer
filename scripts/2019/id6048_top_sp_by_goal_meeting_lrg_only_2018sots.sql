with subset as (
  select 
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bcpy.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_py, 
    fit.fit_for_ia,
    fit.fit_for_ia_cost,
    fitpy.fit_for_ia_cost as fit_for_ia_cost_py,
    dd.num_students,
    dsp.primary_sp,
    du.switcher,
    dlpy.most_recent_ia_contract_end_date as most_recent_ia_contract_end_date_py,
    dl.most_recent_ia_contract_end_date as most_recent_ia_contract_end_date_cy
  --to filter for clean districts
  FROM ps.districts_fit_for_analysis fit
  --to filter for Traditional districts in universe, and student quantification
  JOIN ps.districts dd
  ON fit.district_id = dd.district_id
  AND fit.funding_year = dd.funding_year
  --to determine if the district is meeting goals
  JOIN ps.districts_bw_cost bc
  ON fit.district_id = bc.district_id
  AND fit.funding_year = bc.funding_year
  --to filter for Traditional districts in universe, and student quantification
  LEFT JOIN ps.districts ddpy
  ON fit.district_id = ddpy.district_id
  AND fit.funding_year - 1 = ddpy.funding_year
  --to determine if the district is meeting goals
  LEFT JOIN ps.districts_bw_cost bcpy
  ON fit.district_id = bcpy.district_id
  AND fit.funding_year - 1 = bcpy.funding_year
  --to determine if the district was eligible for upgrade last year
  LEFT JOIN ps.districts_fit_for_analysis fitpy
  ON fit.district_id = fitpy.district_id
  AND fit.funding_year - 1 = fitpy.funding_year
  --to determine if district has primary provider
  LEFT JOIN ps.districts_sp_assignments dsp
  ON fit.district_id = dsp.district_id
  AND fit.funding_year = dsp.funding_year
  --to determine if district switched to provider
  LEFT JOIN ps.districts_upgrades du
  ON fit.district_id = du.district_id
  AND fit.funding_year = du.funding_year
  --to determine if contract expiring
  left JOIN ps.districts_lines dl
  ON fit.district_id = dl.district_id
  AND fit.funding_year = dl.funding_year
  --to determine if last year contract expiring
  left JOIN ps.districts_lines dlpy
  ON fit.district_id = dlpy.district_id
  AND fit.funding_year-1 = dlpy.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
),

newly_meeting as (
  select 
    round(sample_groups.num_students*sample_pop.population_students::numeric/sample_pop.sample_students,0)- 
       meeting_2018_pop.students_meeting_100kbps 
        as num_students
    from (
      select 
        meeting_2014_goal_no_oversub,
        sum(num_students) as num_students
      from subset
      where fit_for_ia = true
      group by 1
    ) sample_groups
    join (
      select 
        sum(num_students) as population_students,
        sum(num_students) FILTER (WHERE fit_for_ia = true) as sample_students
      from subset
    ) sample_pop  
    on true
    left join (
      select students_meeting_100kbps
      from ps.state_snapshot_frozen_sots
      where funding_year = 2018
      and state_code = 'ALL'
    ) meeting_2018_pop
    on true
    where sample_groups.meeting_2014_goal_no_oversub = true
),

agg_sp as (
  select 
    primary_sp,
    meeting_2014_goal_no_oversub,
    sum(num_students) as num_students,
    count(*) as num_districts,
    sum(num_students) FILTER (where most_recent_ia_contract_end_date_cy <= '2019-06-30'::date) 
      as num_students_expiring_contracts_cy,
    count(*)  FILTER (where most_recent_ia_contract_end_date_cy <= '2019-06-30'::date) 
      as num_districts_expiring_contracts_cy,
    sum(num_students) FILTER (where most_recent_ia_contract_end_date_cy is not null) 
      as num_students_expiring_contracts_sample_cy,
    count(*)  FILTER (where most_recent_ia_contract_end_date_cy is not null) 
      as num_districts_expiring_contracts_sample_cy,
    sum(num_students) FILTER (where most_recent_ia_contract_end_date_py <= '2019-06-30'::date) 
      as num_students_expiring_contracts,
    count(*)  FILTER (where most_recent_ia_contract_end_date_py <= '2019-06-30'::date) 
      as num_districts_expiring_contracts,
    sum(num_students) FILTER (where most_recent_ia_contract_end_date_py is not null) 
      as num_students_expiring_contracts_sample,
    count(*)  FILTER (where most_recent_ia_contract_end_date_py is not null) 
      as num_districts_expiring_contracts_sample,
    sum(num_students) FILTER (where switcher) as num_students_switched,
    count(*)  FILTER (where switcher) as num_districts_switched,
    sum(num_students) FILTER (where switcher is not null) as num_students_switched_sample,
    count(*)  FILTER (where switcher is not null) as num_districts_switched_sample
  from subset
  where fit_for_ia_cost = true
  and fit_for_ia_cost_py = true
  and meeting_2014_goal_no_oversub_py = false
  and num_students > 9000
  group by 1,2
),

top_sp as (
  select 
    primary_sp,
    meeting_2014_goal_no_oversub,
    num_students,
    num_districts,
    num_students_switched,
    num_students_switched_sample,
    num_districts_switched,
    num_districts_switched_sample,
    num_students_expiring_contracts,
    num_students_expiring_contracts_sample,
    num_districts_expiring_contracts,
    num_districts_expiring_contracts_sample,
    num_students_expiring_contracts_cy,
    num_students_expiring_contracts_sample_cy,
    num_districts_expiring_contracts_cy,
    num_districts_expiring_contracts_sample_cy,
    ROW_NUMBER() OVER (PARTITION BY primary_sp is null, 
                                    num_districts > 1, 
                                    meeting_2014_goal_no_oversub
                      ORDER BY num_students DESC) as rank_sp 
  from agg_sp
),

top_sp_within_group as (
  select
    meeting_2014_goal_no_oversub,
    newly_meeting.num_students as num_students_newly_meeting, 
    sum(top_sp.num_students::numeric) filter (where rank_sp <= 3 
                                              and primary_sp is not null
                                              and num_districts > 1)
      as num_students_connected_top_3,
    sum(top_sp.num_districts::numeric) filter (where rank_sp <= 3 
                                              and primary_sp is not null
                                              and num_districts > 1)
      as num_districts_connected_top_3,
    array_agg(primary_sp) filter (where rank_sp <= 3 
                                  and primary_sp is not null
                                  and num_districts > 1)
      as top_3,
    sum(num_districts_switched::numeric) filter ( where rank_sp <= 3 
                                                  and primary_sp is not null
                                                  and num_districts > 1)/
    sum(num_districts_switched_sample::numeric) filter (where rank_sp <= 3 
                                                        and primary_sp is not null
                                                        and num_districts > 1)
      as pct_districts_connected_top_3_switched,
    sum(num_students_switched::numeric) filter (where rank_sp <= 3 
                                                and primary_sp is not null
                                                and num_districts > 1)/
    sum(num_students_switched_sample::numeric) filter ( where rank_sp <= 3 
                                                        and primary_sp is not null
                                                        and num_districts > 1)
      as pct_students_connected_top_3_switched,
    sum(num_districts_expiring_contracts::numeric) filter ( where rank_sp <= 3 
                                                            and primary_sp is not null
                                                            and num_districts > 1)/
    sum(num_districts_expiring_contracts_sample::numeric) filter (where rank_sp <= 3 
                                                                  and primary_sp is not null
                                                                  and num_districts > 1)
      as pct_districts_connected_top_3_expiring_contracts,
    sum(num_students_expiring_contracts::numeric) filter (where rank_sp <= 3 
                                                          and primary_sp is not null
                                                          and num_districts > 1)/
    sum(num_students_expiring_contracts_sample::numeric) filter ( where rank_sp <= 3 
                                                                  and primary_sp is not null
                                                                  and num_districts > 1)
      as pct_students_connected_top_3_expiring_contracts,
    sum(num_districts_expiring_contracts_cy::numeric) filter (where rank_sp <= 3 
                                                              and primary_sp is not null
                                                              and num_districts > 1)/
    sum(num_districts_expiring_contracts_sample_cy::numeric) filter ( where rank_sp <= 3 
                                                                      and primary_sp is not null
                                                                      and num_districts > 1)
      as pct_districts_connected_top_3_expiring_contracts_cy,
    sum(num_students_expiring_contracts_cy::numeric) filter ( where rank_sp <= 3 
                                                              and primary_sp is not null
                                                              and num_districts > 1)/
    sum(num_students_expiring_contracts_sample_cy::numeric) filter (where rank_sp <= 3 
                                                                    and primary_sp is not null
                                                                    and num_districts > 1)
      as pct_students_connected_top_3_expiring_contracts_cy,
    sum(top_sp.num_students::numeric) filter (where rank_sp <= 10
                                              and primary_sp is not null
                                              and num_districts > 1)
      as num_students_connected_top_10
  from top_sp
  left join newly_meeting
  on top_sp.meeting_2014_goal_no_oversub = true
  group by 1,2
)

select 
  'same group' as group,
  *
from top_sp_within_group

  UNION

select
  'opposite group' as group,
  top_sp.meeting_2014_goal_no_oversub,
  top_sp_within_group.num_students_newly_meeting,
  sum(top_sp.num_students::numeric) filter ( where top_sp.primary_sp  = any(top_sp_within_group.top_3))
    as num_students_connected_top_3,
  sum(top_sp.num_districts::numeric) filter ( where top_sp.primary_sp  = any(top_sp_within_group.top_3))
    as num_districts_connected_top_3,
  array_agg(top_sp.primary_sp) filter ( where top_sp.primary_sp  = any(top_sp_within_group.top_3))
    as top_3,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
from top_sp
join top_sp_within_group
on top_sp_within_group.meeting_2014_goal_no_oversub = false
where top_sp.meeting_2014_goal_no_oversub = true
group by 2, 3
