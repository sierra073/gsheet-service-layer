with all_yrs as (
	SELECT dd.district_id, 
	dd.funding_year,
	fit.fit_for_ia,
	fit.fit_for_ia_cost,
	bc.meeting_2018_goal_oversub,
	bc.ia_monthly_cost_total,
	bc.ia_monthly_cost_per_mbps


	FROM ps.districts dd 

 	JOIN ps.districts_fit_for_analysis fit
 	ON fit.district_id = dd.district_id
 	AND fit.funding_year = dd.funding_year

 	JOIN ps.districts_bw_cost bc
	ON dd.district_id = bc.district_id
	AND dd.funding_year = bc.funding_year

	WHERE fit.fit_for_ia = true
	and fit.fit_for_ia_cost = true 
	and dd.district_type = 'Traditional'
	and dd.in_universe = true 

)

SELECT currentyear.district_id,
currentyear.funding_year,
currentyear.ia_monthly_cost_total as new_ia_monthly_cost_total,
olderyear.ia_monthly_cost_total as old_ia_monthly_cost_total,
(currentyear.ia_monthly_cost_total - olderyear.ia_monthly_cost_total)/olderyear.ia_monthly_cost_total
as percent_mrc_increase,
case 
	when (currentyear.ia_monthly_cost_total - olderyear.ia_monthly_cost_total)/olderyear.ia_monthly_cost_total > .05
		then 'Cost Increase'
	when (currentyear.ia_monthly_cost_total - olderyear.ia_monthly_cost_total)/olderyear.ia_monthly_cost_total < -.05
		then 'Cost Decrease'
	else 'No Cost Change'
end as cost_change,
currentyear.ia_monthly_cost_per_mbps as new_cost_per_mbps,
olderyear.ia_monthly_cost_per_mbps as old_cost_per_mbps

FROM all_yrs currentyear 

JOIN all_yrs olderyear 
ON currentyear.district_id = olderyear.district_id
AND (currentyear.funding_year - 1) = olderyear.funding_year 

WHERE currentyear.funding_year >= 2017
AND currentyear.meeting_2018_goal_oversub = TRUE 
AND olderyear.meeting_2018_goal_oversub = FALSE

/* looked into the 7 cases where a district has free internet in one or more year and i don't think they should be included in this analysis*/
AND  olderyear.ia_monthly_cost_total > 0 
AND currentyear.ia_monthly_cost_total > 0
