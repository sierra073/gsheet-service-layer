select 
  funding_year,
  pre_discount_eligible_amount, 
  commitment_amount_request
from dm.cost_summary 
where funding_year in (2018,2019)
and in_universe = true
and category_of_spend = 'Internet'