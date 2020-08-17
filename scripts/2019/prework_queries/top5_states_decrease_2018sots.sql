--get each state in each funding year's median cost per mbps
with lookup as (
select 
  d.state_code,
  d.funding_year,
  median(dbc.ia_monthly_cost_per_mbps) as median_cost_per_mbps
  
from ps.districts d

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

where d.in_universe = true
and dfa.fit_for_ia_cost = true
and d.state_code != 'AK'
group by 1,2),

--calculate the percent change in median cost per mbps from 2015 to 2019, and from 2018 to 2019 for each state
lookup2 as (
select l.*,

case when l15.median_cost_per_mbps > 0
then (l18.median_cost_per_mbps - l15.median_cost_per_mbps)::numeric/l15.median_cost_per_mbps
end as pchg_cost_mbps_15,

case when l17.median_cost_per_mbps > 0
then (l18.median_cost_per_mbps - l17.median_cost_per_mbps)::numeric/l17.median_cost_per_mbps
end as pchg_cost_mbps_17

from lookup l

left join
(select * from lookup where funding_year = 2019) l18
on l.state_code = l18.state_code

left join
(select * from lookup where funding_year = 2018) l17
on l.state_code = l17.state_code

left join
(select * from lookup where funding_year = 2015) l15
on l.state_code = l15.state_code),

--based on these percent changes, rank each state for each (ascending, so the more negative the change the better)
lookup3 as (
select l2.*,
row_number() over (order by pchg_cost_mbps_15) as rank_15,
row_number() over (order by pchg_cost_mbps_17) as rank_17
from 
(select distinct state_code, pchg_cost_mbps_15, pchg_cost_mbps_17
from lookup2) l2
),

top5 as (
select distinct '2015-2019' as period,
state_code, rank_15 as rank, pchg_cost_mbps_15 as pchg_cost_mbps
from lookup3
where rank_15 <= 5
union
select distinct '2018-2019' as period,
state_code, rank_17 as rank, pchg_cost_mbps_17 as pchg_cost_mbps
from lookup3
where rank_17 <= 5)

--select each state's cost per mbps each year for each period, grouping (median-ing) the others besides top 5

--2015-2019
select 
'2015-2019' as period,
'Other' as state_code,
funding_year,
median_cost_per_mbps
from 
(select 
  d.funding_year,
  median(dbc.ia_monthly_cost_per_mbps) as median_cost_per_mbps
  
from ps.districts d

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

where d.in_universe = true
and dfa.fit_for_ia_cost = true
and d.state_code not in (
select state_code from top5
where period = '2015-2019')
group by 1) l

union

select 
'2015-2019' as period,
state_code,
funding_year,
median_cost_per_mbps
from lookup
where state_code in (
select state_code from top5
where period = '2015-2019')

union

--2018-2019
select 
'2018-2019' as period,
'Other' as state_code,
funding_year,
median_cost_per_mbps
from 
(select 
  d.funding_year,
  median(dbc.ia_monthly_cost_per_mbps) as median_cost_per_mbps
  
from ps.districts d

join ps.districts_fit_for_analysis dfa
on d.district_id= dfa.district_id
and d.funding_year = dfa.funding_year

join ps.districts_bw_cost dbc
on d.district_id= dbc.district_id
and d.funding_year = dbc.funding_year

where d.in_universe = true
and dfa.fit_for_ia_cost = true
and d.funding_year in (2018, 2019)
and d.state_code not in (
select state_code from top5
where period = '2018-2019')
group by 1) l

union

select 
'2018-2019' as period,
state_code,
funding_year,
median_cost_per_mbps
from lookup
where state_code in (
select state_code from top5
where period = '2018-2019')
and funding_year in (2018, 2019)

order by 1,2,3