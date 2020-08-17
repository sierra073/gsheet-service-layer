SELECT CASE
      WHEN dl.most_recent_ia_contract_end_date <= '2019-06-30'
        THEN 2019
      WHEN dl.most_recent_ia_contract_end_date <= '2020-06-30'
        THEN 2020
      WHEN dl.most_recent_ia_contract_end_date <= '2021-06-30'
        THEN 2021
      ELSE 2022 end as ia_contract_end_date,
      COUNT(*)
FROM ps.districts_lines dl

JOIN ps.districts d
ON dl.district_id = d.district_id
AND dl.funding_year = d.funding_year

JOIN ps.districts_bw_cost bc
ON d.district_id = bc.district_id
AND d.funding_year = bc.funding_year

WHERE d.funding_year = 2019
AND d.in_universe = true
AND d.district_type = 'Traditional'
AND dl.most_recent_ia_contract_end_date is not null
and d.num_students > 9000
and bc.meeting_2014_goal_no_oversub = false

GROUP BY 1

ORDER BY 1
