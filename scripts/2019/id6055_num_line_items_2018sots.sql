select count(distinct dli.line_item_id)
from ps.districts_line_items dli
join ps.districts d
on dli.district_id = d.district_id
and dli.funding_year = d.funding_year
join ps.line_items li
on dli.line_item_id = li.line_item_id
and dli.funding_year = li.funding_year
where d.in_universe = true
and d.district_type = 'Traditional'
and li.exclude_labels = 0
and li.broadband = true
and dli.funding_year = 2019