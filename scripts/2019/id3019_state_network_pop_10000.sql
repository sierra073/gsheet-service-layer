--State Network States, how many districts have pop >10000?
with base as (
select count(distinct d.district_id) as tot_districts,
  sum(case when d.num_students > 10000
  then 1 else 0
  end) as num_over1k

from ps.districts d
where d.state_code in ('AL','AR','CT','DE','GA','HI','IA','KY','ME','MO','MS','NC','ND','NE','RI','SD','SC','UT','WA','WI','WV','WY') --state network states
and d.funding_year=2019
and d.in_universe = true
and d.district_type = 'Traditional'
)
select (b.num_over1k::numeric / b.tot_districts) as percent
from base b