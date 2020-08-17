with temp as (
select 
  case when dbc2.ia_monthly_cost_per_mbps > dbc.ia_monthly_cost_per_mbps
    then true
  else false end as cost_decrease_indicator,
  
  case when dpr.district_id is not null 
    then true
  else false end as has_peer_deal,
  
  count(d.district_id) as ndistricts
  
from ps.districts d

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

join ps.districts_fit_for_analysis dfa2
on d.district_id= dfa2.district_id
and d.funding_year = dfa2.funding_year + 1

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

join ps.districts_bw_cost dbc2
on d.district_id= dbc2.district_id
and d.funding_year = dbc2.funding_year + 1

left join (
select district_id, funding_year
from ps.districts_peers_ranks 
group by 1,2) dpr
on d.district_id= dpr.district_id
and d.funding_year = dpr.funding_year 

where d.funding_year = 2019
and d.in_universe = true
and dfa.fit_for_ia_cost = true
and dfa2.fit_for_ia_cost = true
and d.state_code != 'AK'
group by 1,2 order by 1,2)

select '1) # districts havent seen a cost decrease (extrap)' as metric,
(sum(case when cost_decrease_indicator=false then ndistricts end)::numeric /
sum(ndistricts)) *
(select count(*)
from ps.districts where funding_year = 2019 and in_universe = true) as num

from temp
group by 1

union

select '2) % districts havent seen a cost decrease' as metric,
(sum(case when cost_decrease_indicator=false then ndistricts end)::numeric /
sum(ndistricts)) as num

from temp
group by 1

union

select '3) even though % of them have a peer deal' as metric,
(sum(case when has_peer_deal=true then ndistricts end)::numeric /
sum(ndistricts)) as num

from temp
where cost_decrease_indicator=false
group by 1

order by metric