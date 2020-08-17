select dd.funding_year,
count(dd.district_id) as districts,
round(sum(bc.ia_monthly_cost_total)/sum(bc.ia_bw_mbps_total),2) as weighted_avg_cost_per_mbps,
round(median(bc.ia_monthly_cost_per_mbps),2) as median_cost_per_mbps

FROM ps.districts_frozen_sots dd 

JOIN ps.districts_fit_for_analysis_frozen_sots fit
ON fit.district_id = dd.district_id
AND fit.funding_year = dd.funding_year

JOIN (select bw.funding_year,
bw.district_id,
bw.ia_monthly_cost_total,
bw.ia_bw_mbps_total,
bw.ia_monthly_cost_per_mbps,
case
	when d.size in ('Tiny','Small')
		then bw.ia_bandwidth_per_student_kbps >= 1000
	when d.size in ('Medium','Large','Mega')
		then bw.ia_bandwidth_per_student_kbps >= 700
end as meeting_2018_goal_oversub

from ps.districts_bw_cost_frozen_sots bw

join ps.districts_frozen_sots d 
on bw.district_id = d.district_id 
and bw.funding_year = d.funding_year) 
bc 
on bc.district_id = dd.district_id 
and bc.funding_year = dd.funding_year

WHERE dd.district_type = 'Traditional'
and fit.fit_for_ia = true
and fit.fit_for_ia_cost = true 
and bc.meeting_2018_goal_oversub = true 

group by dd.funding_year


union 

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
and dd.funding_year = 2019

group by dd.funding_year

order by funding_year
