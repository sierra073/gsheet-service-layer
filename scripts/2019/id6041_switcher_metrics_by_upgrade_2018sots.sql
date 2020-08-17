with subset as (
  select 
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bcpy.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub_py, 
    fit.fit_for_ia,
    fitpy.fit_for_ia as fit_for_ia_py,
    dd.num_students,
    dsp.primary_sp,
    du.upgrade_indicator,
    du.switcher  
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
  --to determine districts providers
  LEFT JOIN ps.districts_sp_assignments dsp
  ON fit.district_id = dsp.district_id
  AND fit.funding_year = dsp.funding_year
  --to determine if districts switched
  LEFT JOIN ps.districts_upgrades du
  ON fit.district_id = du.district_id
  AND fit.funding_year = du.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
)

select
  case
    when upgrade_indicator
      then 'upgrade'
    else 'no upgrade'
  end as status,
  sum(1::numeric) filter (where switcher = true)/sum(1::numeric) 
    as pct_districts_switched,
  sum(1::numeric) as sample
from subset
where fit_for_ia = true
and fit_for_ia_py = true
and switcher is not null
group by 1

UNION

select
  'overall' as status,
  sum(1::numeric) filter (where switcher = true)/sum(1::numeric) 
    as pct_districts_switched,
  sum(1::numeric) as sample
from subset
where fit_for_ia = true
and fit_for_ia_py = true
and switcher is not null