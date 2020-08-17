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
  sp_prev.primary_sp as sp_prev

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
    WHEN bc.bw_current > bc.bw_prev then True else False
  END as bw_upgrade,
  CASE
    WHEN bc.cost_current < bc.cost_prev then True else False
  END as cost_upgrade,
  CASE
    WHEN exp_prev <= CONCAT(bc.funding_year::varchar, '-06-30')::date then True else False
  END as expired_this_year,
  CASE
    WHEN (bw_current != bw_prev) OR (sp_current != sp_prev) OR (exp_current != exp_prev) then True else False
  END as new_contract-- this assumes that total cost could change w same contract but bw, sp, and exp persist


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

SELECT
  funding_year,
  count(distinct district_id) as total_districts,
  count(distinct district_id) FILTER (WHERE upgrade_indicator = True) as districts_upgraded,
  count(distinct district_id) FILTER (WHERE upgrade_indicator = False AND cost_upgrade = True) as districts_upgraded_cost_only,
  count(distinct district_id) FILTER (WHERE expired_this_year = True) as total_districts_expired,
  count(distinct district_id) FILTER (WHERE expired_this_year = False) as total_districts_not_expired,

  count(distinct district_id) FILTER (WHERE expired_this_year = True AND upgrade_indicator = True) as districts_expired_upgraded,
  count(distinct district_id) FILTER (WHERE expired_this_year = False AND upgrade_indicator = True) as districts_not_expired_upgraded,
  count(distinct district_id) FILTER (WHERE expired_this_year = True AND upgrade_indicator = False) as districts_expired_not_upgraded,
  count(distinct district_id) FILTER (WHERE expired_this_year = False AND upgrade_indicator = False) as districts_not_expired_not_upgraded,

  round((count(distinct district_id) FILTER (WHERE upgrade_indicator = True))/count(distinct district_id)::decimal*100, 1) as perc_districts_upgraded,
  round((count(distinct district_id) FILTER (WHERE expired_this_year = True AND upgrade_indicator = True))/(count(distinct district_id) FILTER (WHERE expired_this_year = True))::decimal*100, 1) as perc_exp_districts_upg,
  round((count(distinct district_id) FILTER (WHERE expired_this_year = False AND upgrade_indicator = True))/(count(distinct district_id) FILTER (WHERE expired_this_year = False))::decimal*100, 1) as perc_not_exp_districts_upg

FROM result
group by funding_year
