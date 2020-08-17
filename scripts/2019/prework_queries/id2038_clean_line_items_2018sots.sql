select li.line_item_id,
replace(li.frn_complete::varchar,'.','-') as frn_complete
from ps.line_items li

inner join ps.districts_line_items dli 
on dli.line_item_id = li.line_item_id

inner join ps.districts d 
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year


where li.funding_year = 2019
and li.dirty_labels = 0 
and li.exclude_labels = 0 
and li.broadband = true 
and li.erate = true 

and d.in_universe = true 
and d.district_type = 'Traditional'

group by li.line_item_id,
li.frn_complete