with year_meeting as (
SELECT dd.district_id,
dd.num_students,
  CASE
    WHEN bw15.meeting_2018_goal_oversub = true AND ff15.fit_for_ia = true THEN 2015
    WHEN bw16.meeting_2018_goal_oversub = true AND ff16.fit_for_ia = true THEN 2016
    WHEN bw17.meeting_2018_goal_oversub = true AND ff17.fit_for_ia = true THEN 2017
    WHEN bw18.meeting_2018_goal_oversub = true AND ff18.fit_for_ia = true THEN 2018
    WHEN bw19.meeting_2018_goal_oversub = true AND ff19.fit_for_ia = true THEN 2019
    END AS year_meeting_1mbps

FROM ps.districts dd

LEFT JOIN ps.districts_bw_cost bw15
ON dd.district_id = bw15.district_id
AND bw15.funding_year = 2015

LEFT JOIN ps.districts_fit_for_analysis ff15
ON bw15.district_id = ff15.district_id
AND bw15.funding_year = ff15.funding_year

LEFT JOIN ps.districts_bw_cost bw16
ON dd.district_id = bw16.district_id
AND bw16.funding_year = 2016

LEFT JOIN ps.districts_fit_for_analysis ff16
ON bw16.district_id = ff16.district_id
AND bw16.funding_year = ff16.funding_year

LEFT JOIN ps.districts_bw_cost bw17
ON dd.district_id = bw17.district_id
AND bw17.funding_year = 2017

LEFT JOIN ps.districts_fit_for_analysis ff17
ON bw17.district_id = ff17.district_id
AND bw17.funding_year = ff17.funding_year

LEFT JOIN ps.districts_bw_cost bw18
ON dd.district_id = bw18.district_id
AND bw18.funding_year = 2018

LEFT JOIN ps.districts_fit_for_analysis ff18
ON bw18.district_id = ff18.district_id
AND bw18.funding_year = ff18.funding_year

JOIN ps.districts_bw_cost bw19
ON dd.district_id = bw19.district_id
AND bw19.funding_year = dd.funding_year

JOIN ps.districts_fit_for_analysis ff19
ON bw19.district_id = ff19.district_id
AND bw19.funding_year = ff19.funding_year

WHERE dd.funding_year = 2019
and dd.in_universe = true
and dd.district_type = 'Traditional'
and bw19.meeting_2018_goal_oversub = true
),

counts as (
SELECT dd.funding_year,
sum(dd.num_students) as students_population,
sum(dd.num_students) filter (where ff.fit_for_ia = true) as students_sample,
sum(dd.num_students) filter (where yy.year_meeting_1mbps is not null) as students_sample_meeting_2018,

count(distinct dd.district_id) as districts_population,
count(distinct dd.district_id) filter (where ff.fit_for_ia = true) as districts_sample,
count(distinct yy.district_id) filter (where yy.year_meeting_1mbps is not null) as districts_sample_meeting_2018

FROM ps.districts dd

JOIN ps.districts_fit_for_analysis ff
ON dd.district_id = ff.district_id
AND dd.funding_year = ff.funding_year

LEFT JOIN  year_meeting yy
ON dd.district_id = yy.district_id
AND dd.funding_year = yy.year_meeting_1mbps

WHERE dd.in_universe = true
AND dd.district_type = 'Traditional'
GROUP BY 1
),

extrapolations as(
SELECT
  funding_year,
    round(students_sample_meeting_2018::numeric/students_sample*students_population) as students_meeting_2018,
    round(districts_sample_meeting_2018::numeric/districts_sample*districts_population) as districts_meeting_2018
  FROM counts
)


SELECT funding_year,
students_meeting_2018,
districts_meeting_2018

FROM extrapolations


WHERE funding_year != 2015

ORDER BY funding_year asc
