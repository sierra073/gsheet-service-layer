with sv_end AS (
select dl.district_id,
case
  when dl.most_recent_ia_contract_end_date <= '2019-06-30'
    then 1
  when dl.most_recent_ia_contract_end_date <= '2019-06-30'
    then 2
  when dl.most_recent_ia_contract_end_date <= '2020-06-30'
    then 3
  when dl.most_recent_ia_contract_end_date <= '2021-06-30'
    then 4
  when dl.most_recent_ia_contract_end_date <= '2022-06-30'
    then 5
  when dl.most_recent_ia_contract_end_date <= '2023-06-30'
    then 6
  when dl.most_recent_ia_contract_end_date <= '2024-06-30'
    then 7
end as contract_end_time

from ps.districts_lines dl

JOIN ps.districts d
ON dl.district_id = d.district_id
AND dl.funding_year = d.funding_year

JOIN ps.districts_bw_cost bc
ON dl.district_id = bc.district_id
AND dl.funding_year = bc.funding_year

JOIN ps.districts_fit_for_analysis fit
ON dl.district_id = fit.district_id
AND dl.funding_year = fit.funding_year

where d.in_universe
and d.district_type = 'Traditional'
and  bc.meeting_2014_goal_no_oversub = FALSE
and fit.fit_for_ia = true
and dl.funding_year = 2019
AND d.num_students <9000
)

SELECT meeting_2014_goal_no_oversub,
      COUNT(CASE WHEN se.contract_end_time = 2 THEN 1 ELSE NULL END) as small_districts_contract_exp,
      COUNT(*) AS small_districts
FROM sv_end se

JOIN ps.districts_bw_cost bc
ON se.district_id = bc.district_id
AND  bc.funding_year = 2019

JOIN ps.districts_fit_for_analysis fit
ON se.district_id = fit.district_id
AND fit.funding_year = 2019


WHERE fit.fit_for_ia = true

GROUP BY meeting_2014_goal_no_oversub
