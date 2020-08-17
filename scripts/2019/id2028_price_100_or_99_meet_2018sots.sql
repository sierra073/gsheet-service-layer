with districts as (
	select dd.district_id,
	dd.funding_year,
	dd.num_students,
	bc.projected_bw_fy2019_cck12,
	bc.ia_monthly_cost_total,
	(bc.ia_monthly_cost_total/bc.projected_bw_fy2019_cck12) as cost_per_mbps_meeting

	from ps.districts dd 

	join ps.districts_bw_cost bc 
	on dd.district_id = bc.district_id
	and dd.funding_year = bc.funding_year

	join ps.districts_fit_for_analysis fit 
	on dd.district_id = fit.district_id
	and dd.funding_year = fit.funding_year

	/*noticed some outliers where larger districts had a budget of $40 and that this was due to them having a free upstream + backbone costs 
	or some other free/restricted cost scenario. since this analysis might be using the minimum budget i wanted to remove these */
	left join (select dli.district_id 

		from ps.districts_line_items dli

		join ps.line_items li
		on dli.line_item_id = li.line_item_id

		where li.total_cost = 0
		and li.exclude_labels = 0
		and li.purpose in ('internet','upstream','isp')

		group by dli.district_id) zero_cost_recip
	on zero_cost_recip.district_id = dd.district_id

	where dd.in_universe = true 
	and dd.district_type = 'Traditional'
	and dd.funding_year = 2019
	and fit.fit_for_ia_cost = true 
	and bc.meeting_2018_goal_oversub = false 
	and bc.ia_monthly_cost_total > 0

	and zero_cost_recip.district_id is null
)


select median(cost_per_mbps_meeting) as median_cost_per_mbps_meeting,
sum(ia_monthly_cost_total)/sum(projected_bw_fy2019_cck12) as weighted_avg_cost_per_mbps_meeting,
min(cost_per_mbps_meeting) as min_cost_per_mbps_100p_meeting,
percentile_cont (.01) within group (order by cost_per_mbps_meeting) as percentile01_cost_per_mbps_99p_meeting


from districts