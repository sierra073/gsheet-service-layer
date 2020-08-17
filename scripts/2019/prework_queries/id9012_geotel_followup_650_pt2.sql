select distinct
  d.district_id,
  li.parent_name,
  sum(CASE
        WHEN  (li.purpose IN ('internet', 'isp', 'upstream', 'backbone')
          OR  lil.com_info_labels > 0)
        AND   lil.exclude_labels = 0
        AND		li.erate = TRUE
          THEN  dli.total_cost
        ELSE  0
      END) AS  ia_annual_cost_erate

from ps.districts d

join ps.districts_line_items dli
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

join ps.line_items li
on dli.line_item_id = li.line_item_id
and dli.funding_year = li.funding_year

left join dwh.ft_line_item_labels lil
on li.line_item_id = lil.line_item_id
and li.funding_year = lil.funding_year

where d.funding_year = 2019
and d.in_universe = true
and d.district_type = 'Traditional'

group by
d.district_id,
li.parent_name
