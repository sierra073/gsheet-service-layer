
 with districts_c2 as (
 select d.state_code,
       d.funding_year,
       d.district_id,
       d.num_students,
       dw.budget_post,
       dw.remaining_post,
       dw.remaining_post / dw.budget_post as perc_remaining_postdiscount
       
 
from ps.districts_wifi dw

left join ps.districts d
on dw.district_id = d.district_id
and dw.funding_year = d.funding_year

left join ps.districts_fit_for_analysis dffa
on d.district_id = dffa.district_id
and d.funding_year = dffa.funding_year

where d.district_type = 'Traditional'
and d.in_universe = true
and d.funding_year = 2019),

states_c2 as 
(select 
      a.state_code,
       count(a.district_id) filter (where a.perc_remaining_postdiscount < .5) as num_districts_with_all_c2_remaining,
       count(a.district_id) as num_districts_total,
       count(a.district_id) filter (where a.perc_remaining_postdiscount < .5)::float / count(a.district_id)::float as percent_districts_that_used_50percent_or_more_of_c2_funds
       

from districts_c2 a

where state_code not in ('DC')

group by state_code
order by state_code)

select count(state_code)
from states_c2

where percent_districts_that_used_50percent_or_more_of_c2_funds >= .75
