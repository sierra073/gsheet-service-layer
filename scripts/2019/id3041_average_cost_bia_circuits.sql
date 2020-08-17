with base as(
select dli.district_id,
  dli.funding_year,
  dli.bandwidth_received/dli.num_lines as circ_size,
  dli.rec_cost/dli.num_lines as circ_cost
  --generate_series(1,dli.num_lines)

from ps.districts_line_items dli

join ps.districts d
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

join ps.line_items li
on li.line_item_id = dli.line_item_id
and li.funding_year = dli.funding_year

where dli.purpose = 'internet'
and d.in_universe = true
and d.district_type = 'Traditional'
and dli.rec_cost != 0
and li.connect_category in ('Fiber','Lit Fiber')
and d.state_code != 'AK'
and li.dirty_labels = 0
and li.dirty_cost_labels = 0
and li.exclude_labels = 0
and li.erate = true

)

select b.funding_year,
  b.circ_size,
  median(b.circ_cost) as median_cost,
  avg(b.circ_cost) as avg_cost
from base b
where b.circ_size in (100,1000,10000)

group by b.funding_year,
  b.circ_size
  
order by circ_size asc