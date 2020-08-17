select distinct li.line_item_id,
  d.state_code,
  d.size,
  dli.funding_year,
  li.total_monthly_cost as li_monthly_cost,
  --li.bandwidth_in_mbps,
  dli.bandwidth_received,
  (li.total_monthly_cost / (dli.num_lines * dli.bandwidth_received)) as rec_cost_per_meg,
  li.service_provider_id,
  ss.state_network,
  sp.parent_name,
  case when d.state_code in ('AK','VA','NM','MN','FL','IL','KS','NV','TN')
    then 'High'
    when d.state_code in ('OH','LA','AZ','MD','NJ','RI','IA','SC')
    then 'Mid'
    else 'Low' end as cost_tier,
    li.num_lines

from ps.districts_line_items dli

join ps.districts d
on d.funding_year = dli.funding_year
and d.district_id = dli.district_id

left join ps.districts_fit_for_analysis dffa
on dffa.district_id = dli.district_id
and dffa.funding_year = dli.funding_year

join ps.line_items li
on li.funding_year = dli.funding_year
and li.line_item_id = dli.line_item_id

join ps.service_providers sp
on sp.service_provider_id = li.service_provider_id
and sp.funding_year = li.funding_year

join ps.states_static ss
on ss.state_code = d.state_code

where dli.purpose = 'isp'
and dffa.fit_for_ia_cost = true
and dli.total_monthly_cost != 0
and d.in_universe=true
--and d.size in ('Large','Mega')
--and d.state_code ='GA'
--and d.funding_year = 2019
--and sp.parent_name = 'Cox'

--group BY
--dli.district_id, d.state_code, d.size,
  --dli.funding_year, dli.total_monthly_cost, dli.bandwidth_received, dli.num_lines,
 -- li.service_provider_id, ss.state_network, sp.parent_name, cost_tier

order by dli.funding_year