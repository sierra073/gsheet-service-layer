--add average students bw rather than districts

select 
  fit.funding_year,
  median(bc.ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps,
  median(bc.ia_bandwidth_per_student_kbps) FILTER (WHERE bc.meeting_2014_goal_no_oversub = false) as median_ia_bandwidth_per_student_kbps_not_meeting,
  median(bc.ia_bandwidth_per_student_kbps) FILTER (WHERE bc.meeting_2014_goal_no_oversub = true) as median_ia_bandwidth_per_student_kbps_meeting

--to filter for clean districts
FROM ps.districts_fit_for_analysis fit

--to filter for Traditional districts in universe
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

group by 1

UNION

select 
  fit.funding_year,
  median(bc.ia_bandwidth_per_student_kbps) as median_ia_bandwidth_per_student_kbps,
  median(bc.ia_bandwidth_per_student_kbps) FILTER (WHERE bc.meeting_2014_goal_no_oversub = false) as median_ia_bandwidth_per_student_kbps_not_meeting,
  median(bc.ia_bandwidth_per_student_kbps) FILTER (WHERE bc.meeting_2014_goal_no_oversub = true) as median_ia_bandwidth_per_student_kbps_meeting
--to filter for clean districts
FROM ps.districts_fit_for_analysis_frozen_sots fit

--to filter for Traditional districts in universe
JOIN ps.districts_frozen_sots dd
ON fit.district_id = dd.district_id
AND fit.funding_year = dd.funding_year

--to determine if the district is meeting goals
JOIN ps.districts_bw_cost_frozen_sots bc
ON fit.district_id = bc.district_id
AND fit.funding_year = bc.funding_year

where dd.district_type = 'Traditional'
and fit.fit_for_ia = true

group by 1

UNION

select 
  2013,
  52 as median_ia_bandwidth_per_student_kbps,
  NULL as median_ia_bandwidth_per_student_kbps_not_meeting,
  NULL as median_ia_bandwidth_per_student_kbps_meeting


order by 1