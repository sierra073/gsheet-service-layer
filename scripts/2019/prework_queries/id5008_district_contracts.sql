
select distinct dli.district_id,
  li.funding_year,
  dli.line_item_id,
  li.frn,
  li.purpose,
  li.connect_type,
  li.connect_category,
  dli.district_applied,
  uli.service_start_date,
  uli.contract_expiration_date
from ps.districts_line_items dli

join ps.line_items li
on li.funding_year = dli.funding_year
and li.line_item_id = dli.line_item_id

join dwh.dt_usac_line_items uli
on uli.funding_year = li.funding_year
and uli.frn = li.frn

join ps.districts d
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

join ps.districts_fit_for_analysis ffa
on ffa.district_id = d.district_id
and ffa.funding_year = d.funding_year

where d.in_universe = True
and d.district_type = 'Traditional'
and li.purpose in ('internet', 'upstream', 'isp')
and ffa.fit_for_ia = True
