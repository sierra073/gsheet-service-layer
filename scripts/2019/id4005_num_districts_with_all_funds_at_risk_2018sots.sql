with agg as (
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
and d.funding_year = 2019
and dw.remaining_post / dw.budget_post = 1)

select count(district_id) as num_districts_with_all_c2_funds_at_risk
from agg

  

