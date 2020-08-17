select 
(sum(dw.budget_post) filter (where dw.funding_year = 2015) - 
  sum(dw.remaining_post) filter (where dw.funding_year = 2015)) as c2_funds_used_2015,
sum(case when c2_received = true then 1 else 0 end) filter (where dw.funding_year = 2015) as num_districts_using_c2_funds_2015,

(sum(dw.budget_post) filter (where dw.funding_year = 2017) - 
  sum(dw.remaining_post) filter (where dw.funding_year = 2017)) -
(sum(dw.budget_post) filter (where dw.funding_year = 2015) - 
  sum(dw.remaining_post) filter (where dw.funding_year = 2015)) as c2_funds_used_2017, 
sum(case when c2_received = true then 1 else 0 end) filter (where dw.funding_year = 2017) as num_districts_using_c2_funds_2017,

  
(sum(dw.budget_post) filter (where dw.funding_year = 2018) - 
  sum(dw.remaining_post) filter (where dw.funding_year = 2018)) -
(sum(dw.budget_post) filter (where dw.funding_year = 2017) - 
  sum(dw.remaining_post) filter (where dw.funding_year = 2017)) as c2_funds_used_2018,
sum(case when c2_received = true then 1 else 0 end) filter (where dw.funding_year = 2018) as num_districts_using_c2_funds_2018,

  
(sum(dw.budget_post) filter (where dw.funding_year = 2019) - 
  sum(dw.remaining_post) filter (where dw.funding_year = 2019)) -
(sum(dw.budget_post) filter (where dw.funding_year = 2018) - 
  sum(dw.remaining_post) filter (where dw.funding_year = 2018)) as c2_funds_used_2019,
sum(case when c2_received = true then 1 else 0 end) filter (where dw.funding_year = 2019) as num_districts_using_c2_funds_2019


from ps.districts_wifi dw

join ps.districts d
on dw.district_id = d.district_id
and dw.funding_year = d.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'

