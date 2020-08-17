select 
  funding_year,
  case
    when in_universe = true
    and category_of_spend = 'Internet'
      then 'Internet'
    when in_universe = true
    and category_of_spend = 'WAN'
      then 'WAN'
    when in_universe = true
    and category_of_spend = 'Cat 2'
      then 'C2'
    when category_of_spend = 'Voice'
      then 'Voice'
    else 'Other'
  end as categoy,
  sum(pre_discount_eligible_amount) as pre_discount_eligible_amount, 
  sum(commitment_amount_request) as commitment_amount_request
from dm.cost_summary 
where funding_year in (2018,2019)
group by 1,2