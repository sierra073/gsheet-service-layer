  select
    fit.district_id,
    dd.name,
    dd.num_students,
    dd.locale,
    dd.size,
    case
      when bc.meeting_2018_goal_oversub = false
        then bc.ia_bandwidth_per_student_kbps
      else bc.ia_bandwidth_per_student_kbps/dd.setda_concurrency_factor
    end as ia_bandwidth_per_student_kbps_adj
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
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
  and fit.fit_for_ia = true
  order by dd.num_students asc