select count(distinct dw.district_id) filter (where dw.c2_received = true),
       sum(dw.budget_post) filter (where dw.funding_year = 2019) - sum(dw.remaining_post) filter (where dw.funding_year = 2019) as c2_funds_used
from ps.districts_wifi dw

join ps.districts d
on d.district_id = dw.district_id
and d.funding_year = dw.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
