select 
  case
    when category_of_spend = 'Voice'
      then 'Voice'
    when in_universe = false
      then 'Other'
    else category_of_spend
  end as category_of_spend,
  sum(commitment_amount_request) as commitment_amount_request
from dm.cost_summary 
where funding_year = 2019
group by 1