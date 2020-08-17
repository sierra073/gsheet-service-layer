with calc as (select li.bandwidth_in_mbps,
count(distinct li.line_item_id) as line_items,
median(li.rec_cost/li.num_lines) as median_rec_cost,
percentile_cont(.3) within group (order by li.rec_cost/li.num_lines) as percentile_30_rec_cost,
case 
	when li.bandwidth_in_mbps = 10000
		then 7500
	when li.bandwidth_in_mbps = 1000
		then 3000
	when li.bandwidth_in_mbps = 500
		then 2750
	when li.bandwidth_in_mbps = 200 
		then 1800
	when li.bandwidth_in_mbps = 100 
		then 1200
	when li.bandwidth_in_mbps = 50
		then 700
end as old_knapsack_rec_cost

from ps.line_items li

inner join (select  dli.line_item_id 

	from ps.districts_line_items dli

	inner join ps.districts d
	on 	dli.district_id = d.district_id
	and dli.funding_year = d.funding_year

	where d.district_type = 'Traditional'
	and d.in_universe = true 
	and d.state_code != 'AK'

	group by dli.line_item_id
) limit_universe 
/*limiting universe without duplicating line items*/
on limit_universe.line_item_id = li.line_item_id

where li.funding_year = 2019
and li.purpose = 'internet'
and li.connect_category = 'Lit Fiber'
and li.dirty_labels = 0 
and li.dirty_cost_labels = 0
and li.exclude_labels = 0
and li.bandwidth_in_mbps in (10000,1000,500,200,100,50)
and li.rec_cost > 0 
and li.erate = true 

group by li.bandwidth_in_mbps)


select 
'30th_percentile' as method,
bandwidth_in_mbps,
percentile_30_rec_cost as cost 


from calc 

union 

select 
'median' as method,
bandwidth_in_mbps,
median_rec_cost as cost 

from calc 

union 

select 
'knapsack' as method,
bandwidth_in_mbps,
old_knapsack_rec_cost as cost 


from calc 

