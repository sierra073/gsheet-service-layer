select  d.state_code,
  case when (li.connect_category in ('Lit Fiber','Fiber'))
    then 'scalable'
    else 'unscalable'
    end as scalable,
  count(case when (dli.bandwidth_received / dli.num_lines) = 50
      then 1
      end) as m50_circs, 
  count(case when (dli.bandwidth_received / dli.num_lines) = 100
      then 1
      end) as m100_circs,
  count(case when (dli.bandwidth_received / dli.num_lines) = 500
      then 1
      end) as m500_circs,
  count(case when (dli.bandwidth_received / dli.num_lines) = 1000
      then 1
      end) as g1_circs,
  count(case when (dli.bandwidth_received / dli.num_lines) = 2000
      then 1
      end) as g2_circs,
  count(case when (dli.bandwidth_received / dli.num_lines) = 5000
      then 1
      end) as g5_circs,
  count(case when (dli.bandwidth_received / dli.num_lines) = 10000
      then 1
      end) as g10_circs
  
from ps.districts_line_items dli

join ps.districts d
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

join ps.line_items li
on li.line_item_id = dli.line_item_id
and li.funding_year = dli.funding_year

join ps.states_static ss
on ss.state_code = d.state_code

where dli.funding_year = 2019
and ss.state_network = true
and dli.purpose = 'upstream'
and d.in_universe = true
and d.district_type = 'Traditional'
--and li.connect_category in ('Lit Fiber','Fiber')
and (dli.bandwidth_received / dli.num_lines) in (50,100,500,1000,2000,5000,10000)

group by d.state_code,
  scalable

order by scalable,
d.state_code