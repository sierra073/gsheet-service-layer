select dd.funding_year,
count(dd.district_id) as districts,
round(sum(bc.ia_monthly_cost_total)/sum(bc.ia_bw_mbps_total),2) as weighted_avg_cost_per_mbps,
round(median(bc.ia_monthly_cost_per_mbps),2) as median_cost_per_mbps

FROM ps.districts dd 

JOIN ps.districts_fit_for_analysis fit
ON fit.district_id = dd.district_id
AND fit.funding_year = dd.funding_year

JOIN ps.districts_bw_cost bc
ON dd.district_id = bc.district_id
AND dd.funding_year = bc.funding_year

WHERE dd.in_universe = true
and dd.district_type = 'Traditional'
and fit.fit_for_ia = true
and fit.fit_for_ia_cost = true 
and bc.meeting_2018_goal_oversub = true 

group by dd.funding_year

order by dd.funding_year
