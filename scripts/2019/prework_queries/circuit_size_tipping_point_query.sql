select
d.district_id,
d.funding_year,
d.district_type,
d.in_universe,
dffa.fit_for_ia,
d.num_students,
dbw.ia_bw_mbps_total,
dbw.projected_bw_fy2018,
dbw.projected_bw_fy2018_cck12,
dbw.meeting_2018_goal_oversub

-- basic district info
FROM ps.districts d

-- district costs and bw
JOIN ps.districts_bw_cost dbw
ON d.district_id = dbw.district_id
AND d.funding_year = dbw.funding_year

-- to check for fit for ia
JOIN ps.districts_fit_for_analysis dffa
ON d.district_id = dffa.district_id
AND d.funding_year = dffa.funding_year

where d.funding_year = 2019
