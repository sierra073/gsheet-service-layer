select dli.district_id,
  li.line_item_id,
  dli.funding_year,
  d.state_code,
  dli.purpose,
  --li.connect_category,
  li.bandwidth_in_mbps,
  li.rec_cost,
  case when li.bandwidth_in_mbps != 0
    then  li.rec_cost / (li.num_lines * li.bandwidth_in_mbps)
    end as rec_cost_per_meg,
  case when li.bandwidth_in_mbps != 0
    then  li.rec_cost / li.num_lines
    end as total_circuit_cost,
  case when d.state_code in ('AL','CT','DE','GA','HI','KY','ME','MO','MS','NC','ND','NE','RI','SD','SC','TN','UT','WA','WV','WY') --state network states
    then true
    else false
    end as state_network,
  li.spin,
  li.parent_name,
  generate_series(1,dli.num_lines)

--select *

from ps.districts_line_items dli

join ps.districts d
on d.district_id=dli.district_id
and d.funding_year=dli.funding_year

join ps.districts_fit_for_analysis dffa
on dffa.district_id=dli.district_id
and dffa.funding_year=dli.funding_year

join ps.line_items li
on li.line_item_id=dli.line_item_id
and li.funding_year=dli.funding_year
and li.connect_category in ('Lit Fiber','Fiber')

right outer join ps.service_providers sp
on sp.funding_year = dli.funding_year
and sp.service_provider_id = li.service_provider_id

where dli.purpose in ('wan','upstream')
and dffa.fit_for_wan_cost=true
and dffa.fit_for_ia_cost=true
and d.in_universe = true
and dli.rec_cost !=0
and sp.consortium = false
and li.bandwidth_in_mbps in (1000,10000)
--and d.state_code != 'AK'
--and d.state_code = 'GA'
and dli.funding_year = 2019

order by total_circuit_cost desc