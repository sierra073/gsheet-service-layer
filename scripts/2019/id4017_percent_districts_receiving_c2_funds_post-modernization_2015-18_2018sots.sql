select count(distinct dw.district_id) filter (where dw.c2_received = true) / 
            count(distinct dw.district_id)::float

from ps.districts_wifi dw

join ps.districts d
on d.district_id = dw.district_id 
and d.funding_year = 2019

where d.district_type = 'Traditional'
and d.in_universe = true

