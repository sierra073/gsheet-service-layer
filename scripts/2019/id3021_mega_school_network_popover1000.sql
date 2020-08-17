with base as (
  select count (distinct d.district_id) as tot_districts,
    count (distinct s.school_id) as tot_schools,
    sum( case when s.num_students >1000
        then 1 else 0
        end) as num_over1k

from ps.districts d

join ps.schools s
on s.funding_year=d.funding_year
and s.district_id=d.district_id

where d.size = 'Mega'
and d.funding_year = 2019
and d.in_universe = true
and d.district_type = 'Traditional'

and d.state_code in ('AL','AR','CT','DE','GA','HI','IA','KY','ME','MO','MS','NC','ND','NE','RI','SD','SC','UT','WA','WI','WV','WY') --state network states
)
select (b.num_over1k::numeric / b.tot_schools) as percent_sch_over1k

from base b