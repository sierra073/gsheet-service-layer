-- all districts who qualify for a peer deal
with dd as (select
	dd.district_id,
	dd.state_code,
	dd.funding_year,
	dd.num_students,
	bc.meeting_2018_goal_oversub,
	bc.meeting_2014_goal_no_oversub,
	bc.ia_monthly_cost_total,
	bc.ia_bw_mbps_total,
	bc.ia_monthly_cost_per_mbps,
	sp.primary_sp

 	FROM ps.districts dd

 	JOIN ps.districts_fit_for_analysis fit
 	ON fit.district_id = dd.district_id
 	AND fit.funding_year = dd.funding_year

 	JOIN ps.districts_bw_cost bc
	ON dd.district_id = bc.district_id
	AND dd.funding_year = bc.funding_year

	JOIN ps.districts_sp_assignments sp
	ON dd.district_id = sp.district_id
	and dd.funding_year = sp.funding_year

 	WHERE dd.in_universe = true
 	AND dd.district_type = 'Traditional'
 	and dd.funding_year = 2019
 	and fit.fit_for_ia = true
 	and fit.fit_for_ia_cost = true

 	),
-- using old methodology get all peer deals (same/better bw and cost) that would bring to 100kb goal
peer_deals_old as (select
	dd.funding_year,
	dd.district_id,
	dd.num_students,
	count(p.peer_id) as deals,
	count(case
		when dd.primary_sp = pd.primary_sp
			then peer_id
		end) as deals_same_sp,
  p.peer_service_provider

	from dd

	inner join ps.districts_peers_ranks  p
	ON dd.district_id = p.district_id
	AND dd.funding_year = p.funding_year

	inner join dd pd
	on pd.district_id = p.peer_id
	and pd.funding_year = p.funding_year

	where pd.ia_bw_mbps_total >= dd.ia_bw_mbps_total
	and pd.ia_monthly_cost_total <= dd.ia_monthly_cost_total
	and pd.ia_bw_mbps_total/dd.num_students >= .1

	and dd.meeting_2014_goal_no_oversub = false
	and dd.ia_monthly_cost_total > 0

	group by dd.funding_year,
	dd.district_id,
	dd.num_students,
	p.peer_service_provider
),

new_peer_deal_agg as (
select distinct funding_year,
  district_id,
  deal,
  CASE
    WHEN sum(case when mapping_source = 'esh' then 1 else 0 end) = count(line_item_id)
    THEN 1 else 0
  END as all_esh_deal,
  CASE
    WHEN sum(case when mapping_source = 'geotel' then 1 else 0 end) > 0
    THEN 1 else 0
  END as any_geotel_deal,
  CASE
    WHEN sum(current_provider::int) = count(line_item_id)
    THEN 1 else 0
  END as all_current_provider_deal,
	sum(bandwidth_in_mbps*circuits) as total_deal_bw,
	sum(circuit_total_monthly_cost*circuits) as total_deal_monthly_cost,
	array_agg(parent_name) as parent_names

from ps.peer_deal_line_items

group by funding_year,
  district_id,
  deal
),

-- using new methodology get all peer deals (same/better bw and cost) that would bring to 100kb goal
peer_deals_new as (select
	dd.funding_year,
	dd.district_id,
	dd.num_students,
	count(distinct p.deal) AS new_deals,
	sum(all_current_provider_deal) as num_new_same_sp,
  sum(all_esh_deal) as num_esh_only_deals,
  sum(any_geotel_deal) as num_geotel_deals,
  unnest(parent_names) as parent_name

	from dd

	JOIN new_peer_deal_agg p
  ON dd.district_id = p.district_id
  AND dd.funding_year = p.funding_year

	where p.total_deal_bw >= dd.ia_bw_mbps_total
	and p.total_deal_monthly_cost <= dd.ia_monthly_cost_total

	and dd.meeting_2014_goal_no_oversub = false
	and dd.ia_monthly_cost_total > 0

	group by dd.funding_year,
	dd.district_id,
	dd.num_students,
	parent_name
)

select distinct dd.district_id,
  dd.state_code,
  sqrt(sum(pd.deals)) as total_deals_old_meth,
  array_remove(array_agg(distinct
            case when pd.peer_service_provider not in (select parent_name from peer_deals_new)
            then pd.peer_service_provider end), NULL) as sp_old_meth_only
  -- array_remove(array_agg(distinct case when pd.peer_service_provider in (select parent_name from peer_deals_new) then pd.peer_service_provider end), NULL) as sp_exists_new_meth

from dd

left join peer_deals_old pd
on dd.district_id = pd.district_id

left join peer_deals_new pnew
on dd.district_id = pnew.district_id

join ps.states_static ss
on ss.state_code = dd.state_code

where dd.meeting_2014_goal_no_oversub = false
and pd.deals > 0
and pnew.district_id is null
and ss.state_network_natl_analysis = False

group by 1,2
