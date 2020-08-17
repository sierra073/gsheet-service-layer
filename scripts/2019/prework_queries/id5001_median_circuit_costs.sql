select li.funding_year,
  'national' as state,
  li.bandwidth_in_mbps::varchar as circuit_size,
  count(distinct li.line_item_id) as line_items,
  median(li.rec_cost/li.num_lines) as median_rec_cost

from ps.line_items li

inner join (select  dli.funding_year, dli.line_item_id

	from ps.districts_line_items dli

	inner join ps.districts d
	on 	dli.district_id = d.district_id
	and dli.funding_year = d.funding_year

	where d.district_type = 'Traditional'
	and d.in_universe = true
	and d.state_code != 'AK'

	group by dli.funding_year, dli.line_item_id
) limit_universe
/*limiting universe without duplicating line items*/
on limit_universe.line_item_id = li.line_item_id
and limit_universe.funding_year = li.funding_year

where li.purpose = 'internet'
and li.connect_category in ('Lit Fiber', 'Fiber')
and li.dirty_labels = 0
and li.dirty_cost_labels = 0
and li.exclude_labels = 0
and li.bandwidth_in_mbps in (10000,1000,500,200,100,50)
and li.rec_cost > 0
and li.erate = true

group by li.bandwidth_in_mbps, li.funding_year

UNION

select li.funding_year,
  'AK' as state,
  li.bandwidth_in_mbps::varchar as circuit_size,
  count(distinct li.line_item_id) as line_items,
  median(li.rec_cost/li.num_lines) as median_rec_cost

from ps.line_items li

inner join (select  dli.funding_year, dli.line_item_id

	from ps.districts_line_items dli

	inner join ps.districts d
	on 	dli.district_id = d.district_id
	and dli.funding_year = d.funding_year

	where d.district_type = 'Traditional'
	and d.in_universe = true
	and d.state_code = 'AK'

	group by dli.funding_year, dli.line_item_id
) limit_universe
/*limiting universe without duplicating line items*/
on limit_universe.line_item_id = li.line_item_id
and limit_universe.funding_year = li.funding_year

where li.purpose = 'internet'
and li.connect_category in ('Lit Fiber', 'Fiber')
and li.dirty_labels = 0
and li.dirty_cost_labels = 0
and li.exclude_labels = 0
and li.bandwidth_in_mbps in (10000,1000,500,200,100,50)
and li.rec_cost > 0
and li.erate = true

group by li.bandwidth_in_mbps, li.funding_year

UNION

select li.funding_year,
  'national' as state,
  'aggregate' as circuit_size,
  count(distinct li.line_item_id) as line_items,
  median(li.rec_cost/li.num_lines) as median_rec_cost

from ps.line_items li

inner join (select  dli.funding_year, dli.line_item_id

	from ps.districts_line_items dli

	inner join ps.districts d
	on 	dli.district_id = d.district_id
	and dli.funding_year = d.funding_year

	where d.district_type = 'Traditional'
	and d.in_universe = true
	and d.state_code != 'AK'

	group by dli.funding_year, dli.line_item_id
) limit_universe
/*limiting universe without duplicating line items*/
on limit_universe.line_item_id = li.line_item_id
and limit_universe.funding_year = li.funding_year

where li.purpose = 'internet'
and li.connect_category in ('Lit Fiber', 'Fiber')
and li.dirty_labels = 0
and li.dirty_cost_labels = 0
and li.exclude_labels = 0
and li.bandwidth_in_mbps in (10000,1000,500,200,100,50)
and li.rec_cost > 0
and li.erate = true

group by li.funding_year

UNION

select c.funding_year,
  'national' as state,
  '$_mbps' as circuit_size,
  null as line_items,
  median(c.ia_monthly_cost_per_mbps) as median_cost_per_mb
from ps.districts_bw_cost c

join ps.districts d
on 	c.district_id = d.district_id
and c.funding_year = d.funding_year

join ps.districts_fit_for_analysis fa
on 	c.district_id = fa.district_id
and c.funding_year = fa.funding_year

where d.district_type = 'Traditional'
and d.in_universe = true
and d.state_code != 'AK'
and fa.fit_for_ia = true

group by c.funding_year

UNION

select c.funding_year,
  'AK' as state,
  '$_mbps' as circuit_size,
  null as line_items,
  median(c.ia_monthly_cost_per_mbps) as median_cost_per_mb
from ps.districts_bw_cost c

join ps.districts d
on 	c.district_id = d.district_id
and c.funding_year = d.funding_year

join ps.districts_fit_for_analysis fa
on 	c.district_id = fa.district_id
and c.funding_year = fa.funding_year

where d.district_type = 'Traditional'
and d.in_universe = true
and d.state_code = 'AK'
and fa.fit_for_ia = true

group by c.funding_year

order by 2 DESC, 1, 3
