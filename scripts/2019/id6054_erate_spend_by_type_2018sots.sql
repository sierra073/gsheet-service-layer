select
  funding_year,
  case
    when category_of_spend in ('Internet', 'WAN')
      then 'Broadband'
    else category_of_spend
  end as category_of_spend,
  sum(pre_discount_eligible_amount) as pre_discount_eligible_amount,
  sum(commitment_amount_request) as commitment_amount_request
from dm.cost_summary 
where in_universe = true
and funding_year = 2019
and category_of_spend not in ('Other', 'Voice')
group by 1,2
order by 1,2
