with dd as (select 
	dd.district_id, 
	dd.funding_year,
	dd.num_students,
	bc.meeting_2018_goal_oversub,
	bc.ia_monthly_cost_total,
	(dd.num_students::numeric * dd.setda_concurrency_factor) as projected_bw_fy2019_no_rounding,
	bc.ia_monthly_cost_per_mbps,
	sp.primary_sp

	
 	FROM ps.districts dd 

 	JOIN ps.districts_fit_for_analysis fit
 	ON fit.district_id = dd.district_id
 	AND fit.funding_year = dd.funding_year

 	JOIN ps.districts_bw_cost bc
	ON dd.district_id = bc.district_id
	AND dd.funding_year = bc.funding_year

	JOIN ps.districts_sp_assignments sp 
	ON dd.district_id = sp.district_id
	and dd.funding_year = sp.funding_year

 	WHERE dd.in_universe = true
 	AND dd.district_type = 'Traditional'
 	and dd.funding_year = 2019
 	and fit.fit_for_ia = true 
 	and fit.fit_for_ia_cost = true

 	),

peer_deals as (select 
	dd.funding_year,
	dd.district_id,
	dd.num_students,
	count(p.peer_id) as deals,
	count(case 
		when dd.primary_sp = pd.primary_sp
			then peer_id
		end) as deals_same_sp,
	count(case 
		when dd.primary_sp = pd.primary_sp
			then peer_id
		end)::numeric/count(p.peer_id) as deals_same_sp_p


	from dd 

	inner join ps.districts_peers_ranks  p
	ON dd.district_id = p.district_id
	AND dd.funding_year = p.funding_year

	inner join dd pd 
	on pd.district_id = p.peer_id
	and pd.funding_year = p.funding_year 

	where p.peer_ia_bw_mbps_total >= dd.projected_bw_fy2019_no_rounding
	and pd.ia_monthly_cost_total <= dd.ia_monthly_cost_total

	and dd.meeting_2018_goal_oversub = false
	and dd.ia_monthly_cost_total > 0 

	group by dd.funding_year,
	dd.district_id,
	dd.num_students
)


select count(dd.district_id) as all_districts,
count(pd.district_id) as districts_w_deals,
median(pd.deals) filter (where pd.district_id is not null) as median_deals,
round(avg(pd.deals) filter (where pd.district_id is not null)) as average_deals,

count(case 
	when deals_same_sp > 0
		then pd.district_id end) as districts_same_sp,

round(count(case 
	when deals_same_sp > 0
		then pd.district_id end)::numeric/count(pd.district_id),2) as districts_same_sp_deal_districts_p,

round(count(case 
	when deals_same_sp > 0
		then pd.district_id end)::numeric/count(dd.district_id),2) as districts_same_sp_all_districts_p


from dd 


left join peer_deals pd
on dd.district_id = pd.district_id

where dd.meeting_2018_goal_oversub = false
and dd.ia_monthly_cost_total > 0 