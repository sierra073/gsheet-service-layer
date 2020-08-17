select
  dli.district_id,
  dli.funding_year,
  avg(uli.num_bids_received) as avg_li_num_bids_received

from ps.districts_line_items dli

join ps.line_items li
on dli.line_item_id = li.line_item_id
and dli.funding_year = li.funding_year

join ps.usac_line_items uli
ON CONCAT(uli.frn::varchar, '.', RIGHT(CONCAT('000',uli.line_item_no),3)) = li.frn_complete
AND uli.funding_year = li.funding_year

where uli.category_of_service::varchar = '1'

group by
dli.district_id, dli.funding_year
order by
dli.district_id, dli.funding_year