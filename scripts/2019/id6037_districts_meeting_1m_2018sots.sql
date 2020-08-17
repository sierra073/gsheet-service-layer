select 
  dd.district_name,
  dd.state_code,
  dd.num_students,
  bc.ia_bw_per_student_kbps
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
and bc.meeting_2018_goal_oversub = true
order by dd.num_students desc