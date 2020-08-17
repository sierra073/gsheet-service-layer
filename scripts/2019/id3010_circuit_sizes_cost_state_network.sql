select dli.district_id,
  li.line_item_id,
  dli.funding_year,
  d.state_code,
  dli.purpose,
  --li.connect_category,
  li.bandwidth_in_mbps,
  li.total_monthly_cost,
  case when li.bandwidth_in_mbps != 0
    then  li.total_monthly_cost / (li.num_lines * li.bandwidth_in_mbps)
    end as rec_cost_per_meg,
  case when d.state_code in ('AL','CT','DE','GA','HI','KY','ME','MO','MS','NC','ND','NE','RI','SD','SC','TN','UT','WA','WV','WY') --state network states
    then true else false
    end as state_network

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

where dli.purpose in ('wan','upstream')
and dffa.fit_for_ia_cost=true