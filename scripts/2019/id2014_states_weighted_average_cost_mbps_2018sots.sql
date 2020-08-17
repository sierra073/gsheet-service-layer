with all_states as (
select 
  d.funding_year,
  d.state_code,
  sum(dbc.ia_monthly_cost_total)/sum(dbc.ia_bw_mbps_total) as weighted_avg_cost_mbps 
  
from ps.districts d

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

where d.in_universe = true
and d.district_type='Traditional'
and d.state_code != 'DC'
and fit_for_ia = true
and fit_for_ia_cost = true
group by 1,2)

select funding_year, count(*) as num_states
from all_states
where weighted_avg_cost_mbps <= 3
group by 1 order by 1



