select d.district_id,
  d.state_code,
  dli.rec_cost,
  dli.purpose,
  ss.state_network

from ps.districts_line_items dli

join ps.districts d
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

join ps.states_static ss
on ss.state_code = d.state_code

where dli.funding_year = 2019
and d.district_type= 'Traditional'
and d.in_universe = true
--and ss.state_network = true
and dli.rec_cost !=0
and dli.purpose != 'backbone'
