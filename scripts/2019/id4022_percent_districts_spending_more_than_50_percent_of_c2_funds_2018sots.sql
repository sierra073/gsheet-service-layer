select count(dw.district_id) filter (where dw.remaining_post/dw.budget_post < .5) / count(dw.district_id)::float

from ps.districts_wifi dw

join ps.districts d
on d.district_id = dw.district_id
and d.funding_year = dw.funding_year 

join ps.districts_bw_cost dbc
on dw.district_id = dbc.district_id
and dw.funding_year = dbc.funding_year

join ps.districts_fit_for_analysis dffa
on dw.district_id = dffa.district_id
and dw.funding_year = dffa.funding_year

where dw.funding_year = 2019
and d.in_universe = true
and d.district_type = 'Traditional'