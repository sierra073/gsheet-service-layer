select dw.funding_year,
       sum(dw.budget - dw.remaining) as c2_spend,
       sum(dw.budget) as budget,
       sum(dw.budget - dw.remaining) / sum(dw.budget) as perc_c2_spend

from ps.districts_wifi dw

join ps.districts d
on d.district_id = dw.district_id
and d.funding_year = dw.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and dw.funding_year = 2018

group by dw.funding_year 


UNION

select dw.funding_year,
       sum(dw.budget - dw.remaining) as c2_spend,
       sum(dw.budget) as budget,
       sum(dw.budget - dw.remaining) / sum(dw.budget) as perc_c2_spend

from ps.districts_wifi dw

join ps.districts d
on d.district_id = dw.district_id
and d.funding_year = dw.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and dw.funding_year = 2019

group by dw.funding_year

order by funding_year