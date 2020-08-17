with district_lkp as (
select distinct dw.district_id 

from ps.districts_wifi dw

join ps.districts d
on d.district_id = dw.district_id 
and d.funding_year = dw.funding_year

where dw.c2_received = true
and d.district_type = 'Traditional'
and d.in_universe = true)

select sum(dw.budget_post - dw.remaining_post) as c2_used

from district_lkp dl

join ps.districts_wifi dw
on dl.district_id = dw.district_id

where dw.funding_year = 2019




