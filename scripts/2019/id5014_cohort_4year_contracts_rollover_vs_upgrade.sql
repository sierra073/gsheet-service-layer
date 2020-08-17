--get
with expiration as (
SELECT
  dd.funding_year,
  dd.district_id,
  dl_current.most_recent_primary_ia_contract_end_date as exp_current,
  dl_prev.most_recent_primary_ia_contract_end_date as exp_prev,
  dl_prev.funding_year as prev_funding_year

FROM ps.districts dd

LEFT JOIN ps.districts_lines dl_current
ON dd.district_id = dl_current.district_id
and dd.funding_year = dl_current.funding_year

LEFT JOIN ps.districts_lines dl_prev
ON dd.district_id = dl_prev.district_id
AND dd.funding_year = dl_prev.funding_year + 1

WHERE dd.in_universe = True
and dd.district_type = 'Traditional'
and dd.funding_year >= 2016
),

bw_cost as (
SELECT
  dd.funding_year,
  dd.district_id,
  bc_current.ia_bw_mbps_total as bw_current,
  bc_current.ia_monthly_cost_total as cost_current,
  sp_current.primary_sp as sp_current,
  bc_prev.funding_year as prev_funding_year,
  bc_prev.ia_bw_mbps_total as bw_prev,
  bc_prev.ia_monthly_cost_total as cost_prev,
  sp_prev.primary_sp as sp_prev,
  bc_current.meeting_2018_goal_no_oversub

FROM ps.districts dd

LEFT JOIN ps.districts_bw_cost bc_current
ON dd.district_id = bc_current.district_id
AND dd.funding_year = bc_current.funding_year

LEFT JOIN ps.districts_bw_cost bc_prev
ON dd.district_id = bc_prev.district_id
AND dd.funding_year = bc_prev.funding_year + 1

LEFT JOIN ps.districts_sp_assignments sp_current
on sp_current.district_id = dd.district_id
and sp_current.funding_year = dd.funding_year

LEFT JOIN ps.districts_sp_assignments sp_prev
on sp_prev.district_id = dd.district_id
and sp_prev.funding_year + 1 = dd.funding_year

WHERE dd.in_universe = True
and dd.district_type = 'Traditional'
and dd.funding_year >= 2016
),

result as (

SELECT
  bc.*,
  e.exp_current,
  e.exp_prev,
  u.upgrade_indicator,
  CASE
    WHEN bc.cost_current < bc.cost_prev then True else False
  END as cost_upgrade,
  CASE
    WHEN exp_prev <= CONCAT(bc.funding_year::varchar, '-06-30')::date then True else False
  END as expired_this_year,
--if its more than a year away but just a 1 year incr from last end date, consider that a 1 year contract
  CASE
    WHEN (exp_current >= CONCAT((bc.funding_year+2)::varchar, '-06-30')::date)
    OR ((exp_current >= CONCAT((bc.funding_year+1)::varchar, '-08-01')::date) AND NOT (exp_current - exp_prev) BETWEEN 363 and 367)
    then True else False
  END as current_contract_multi_year,
  sum(upgrade_indicator::int) over (partition by bc.district_id) as num_years_upgraded,
  count(ffa.funding_year) over (partition by ffa.district_id) as num_years_clean

from bw_cost bc

join expiration e
on e.district_id = bc.district_id
and e.funding_year = bc.funding_year
and e.prev_funding_year = bc.prev_funding_year

JOIN ps.districts_fit_for_analysis ffa
on ffa.district_id = bc.district_id
and ffa.funding_year = bc.funding_year

JOIN ps.districts_upgrades u
on u.district_id = bc.district_id
and u.funding_year = bc.funding_year

where cost_current is not NULL
and cost_prev is not NULL
and sp_current is not NULL
and sp_prev is not NULL
and bw_current is not NULL
and bw_prev is not NULL
and exp_prev is not NULL
and exp_current is not NULL
and bw_current != 0
and bw_prev  != 0
and ffa.fit_for_ia = True
)

select result.funding_year,
  count(result.district_id) as districts,
  count(result.district_id) FILTER (where upgrade_indicator = True) as num_upgraded,
  sum(expired_this_year::int) as num_expired,
  sum(case when current_contract_multi_year = false and upgrade_indicator = False and expired_this_year = True then 1 else 0 end) as num_exp_rolled,
  sum(case when current_contract_multi_year = True and upgrade_indicator = False and expired_this_year = True then 1 else 0 end) as num_exp_multiyr,
  sum(case when r.upgrade_year = 1 then 1 else 0 end) as num_first_time_upg,
  sum(case when r.upgrade_year = 2 then 1 else 0 end) as num_second_upg,
  sum(case when r.upgrade_year = 3 then 1 else 0 end) as num_third_upg,
  --what percent of cohort had an expiring contract each year
  round(sum(expired_this_year::int)/count(result.district_id)::decimal, 2) as perc_expired,
  -- what percent of cohort who didn't upgrade but was eligible signed 1 vs multi-year contract
  round(sum(case when current_contract_multi_year = false and upgrade_indicator = False and expired_this_year = True then 1 else 0 end)/sum(expired_this_year::int)::decimal, 2) as perc_rolled,
  round(sum(case when current_contract_multi_year = True and upgrade_indicator = False and expired_this_year = True then 1 else 0 end)/sum(expired_this_year::int)::decimal, 2) as perc_multi_year,
  --what percent of districts with expiring contracts upgraded, and how long after year 1 of analysis
  round((count(result.district_id) FILTER (where upgrade_indicator = True and expired_this_year = True))/sum(expired_this_year::int)::decimal, 2) as perc_exp_upgraded,
  round(sum(case when r.upgrade_year = 1 and expired_this_year = True then 1 else 0 end)/sum(expired_this_year::int)::decimal, 2) as perc_exp_first_upg,
  round(sum(case when r.upgrade_year = 2 and expired_this_year = True then 1 else 0 end)/sum(expired_this_year::int)::decimal, 2) as perc_exp_second_upg,
  round(sum(case when r.upgrade_year = 3 and expired_this_year = True then 1 else 0 end)/sum(expired_this_year::int)::decimal, 2) as perc_exp_third_upg

from result

inner join (
-- --limit to cohort who expired in 2016 and didnt upgrade but rolled their contract just 1 more year
  SELECT distinct district_id
  FROM result
  where funding_year = 2016
  and expired_this_year = True
  and upgrade_indicator = False
  and current_contract_multi_year = False
) d on result.district_id = d.district_id

-- to identify districts that upgraded multiple years and the actual number that upgraded at least once
left join (
  select res.district_id,
    res.funding_year,
    rank() over (partition by res.district_id order by res.funding_year) as upgrade_year
  from result res
  where res.upgrade_indicator = True
) r
  on r.district_id = result.district_id
  and r.funding_year = result.funding_year

--limit cohort to only those with 4 years of clean data so can compare year-over-year
where result.num_years_clean = 4

group by result.funding_year
order by result.funding_year
