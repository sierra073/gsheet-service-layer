select sum(dw.remaining_post)
from ps.districts_wifi dw

join ps.districts d
on d.district_id = dw.district_id
and d.funding_year = dw.funding_year

where dw.funding_year = 2019
and d.in_universe = true
and d.district_type = 'Traditional'