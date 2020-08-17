with unnest_cck12_peers as (
  select distinct on (p.district_id, p.funding_year)
    p.district_id,
    p.funding_year,
    p.peer_id,
    bw.ia_annual_cost_total as peer_ia_annual_cost_total
  from (
    select 
      district_id,
      funding_year,
      unnest(bandwidth_suggested_districts) as peer_id
    from ps.districts_peers 
  ) p
  join ps.districts_bw_cost bw
  on p.peer_id = bw.district_id
  and p.funding_year = bw.funding_year
  join ps.districts d
  on p.peer_id = d.district_id
  and p.funding_year = d.funding_year
  join ps.districts_fit_for_analysis fit
  on p.peer_id = fit.district_id
  and p.funding_year = fit.funding_year
  where fit.fit_for_ia = true
  and fit.fit_for_ia_cost = true
  and d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
  order by p.district_id, p.funding_year, bw.ia_annual_cost_total desc
),

subset as (
  select 
    fit.district_id,
    dd.name,
    dd.state_code,
    dd.locale,
    bc.meeting_2014_goal_no_oversub,
    bc.meeting_2018_goal_oversub,
    bc.ia_bandwidth_per_student_kbps,
    bc.ia_monthly_cost_total,
    bc.ia_bw_mbps_total,
    bc17.ia_bw_mbps_total as ia_bw_mbps_total_17,
    bc16.ia_bw_mbps_total as ia_bw_mbps_total_16,
    bc15.ia_bw_mbps_total as ia_bw_mbps_total_15,
    bc.ia_monthly_cost_per_mbps,
    bc.ia_annual_cost_total,
    bc.ia_annual_cost_erate,
    bc.ia_funding_requested_erate,
    sp.primary_sp,
    up.year_of_last_upgrade,
    fit.fit_for_ia,
    fit17.fit_for_ia as fit_for_ia_17,
    fit16.fit_for_ia as fit_for_ia_16,
    fit15.fit_for_ia as fit_for_ia_15,
    fit.fit_for_ia_cost,
    dd.num_students,
    dd.size,
    dd.c1_discount_rate,
    pr.peer_id,
    pdd.name as peer_name,
    pdd.state_code as peer_state_code,
    pdd.locale as peer_locale,
    pdd.num_students as peer_num_students,
    pdd.size as peer_size,
    pbc.ia_bw_mbps_total as peer_ia_bw_mbps_total,
    pbc.ia_monthly_cost_total as peer_ia_monthly_cost_total,
    psp.primary_sp as peer_primary_sp,
    cp.peer_ia_annual_cost_total as incr_cost_peer_ia_annual_cost_total,
    cp.peer_id as incr_cost_peer_id,
    dd.outreach_status,
    dd.engagement_status,
    dd.account_owner
  --to filter for clean districts
  FROM ps.districts_fit_for_analysis fit
  --to filter for clean districts, 2018
  LEFT JOIN ps.districts_fit_for_analysis fit17
  ON fit.district_id = fit17.district_id
  AND fit.funding_year-1 = fit17.funding_year 
  --to filter for clean districts, 2017
  LEFT JOIN ps.districts_fit_for_analysis fit16
  ON fit.district_id = fit16.district_id
  AND fit.funding_year-2 = fit16.funding_year
  --to filter for clean districts, 2015
  LEFT JOIN ps.districts_fit_for_analysis fit15
  ON fit.district_id = fit15.district_id
  AND fit.funding_year-3 = fit15.funding_year
  --to filter for Traditional districts in universe, and student quantification
  JOIN ps.districts dd
  ON fit.district_id = dd.district_id
  AND fit.funding_year = dd.funding_year
  --to determine if the district is meeting goals
  JOIN ps.districts_bw_cost bc
  ON fit.district_id = bc.district_id
  AND fit.funding_year = bc.funding_year
  --to determine if the district is meeting goals, 2018
  LEFT JOIN ps.districts_bw_cost bc17
  ON fit.district_id = bc17.district_id
  AND fit.funding_year-1 = bc17.funding_year 
  --to determine if the district is meeting goals, 2017
  LEFT JOIN ps.districts_bw_cost bc16
  ON fit.district_id = bc16.district_id
  AND fit.funding_year-2 = bc16.funding_year
  --to determine if the district is meeting goals, 2015
  LEFT JOIN ps.districts_bw_cost bc15
  ON fit.district_id = bc15.district_id
  AND fit.funding_year-3 = bc15.funding_year
  --to the districts service provider
  JOIN ps.districts_sp_assignments sp
  ON fit.district_id = sp.district_id
  AND fit.funding_year = sp.funding_year
  --cck12 peers
  LEFT JOIN unnest_cck12_peers cp
  ON fit.district_id = cp.district_id
  AND fit.funding_year = cp.funding_year
  --to determine the year of last upgrade
  LEFT JOIN (
    select 
      district_id,
      max(funding_year) as year_of_last_upgrade
    from ps.districts_upgrades
    where upgrade_indicator = true 
    group by 1
  ) up
  ON fit.district_id = up.district_id
  --to determine if the district is meeting goals
  LEFT JOIN ps.districts_peers_ranks pr
  ON fit.district_id = pr.district_id
  AND fit.funding_year = pr.funding_year
  --to filter for clean districts
  LEFT JOIN ps.districts pdd
  ON pr.peer_id = pdd.district_id
  AND pr.funding_year = pdd.funding_year
  --to determine if the district is meeting goals
  LEFT JOIN ps.districts_bw_cost pbc
  ON pr.peer_id = pbc.district_id
  AND pr.funding_year = pbc.funding_year
  --to the districts service provider
  LEFT JOIN ps.districts_sp_assignments psp
  ON pr.peer_id = psp.district_id
  AND pr.funding_year = psp.funding_year
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
)


      select 
        district_id,
        name,
        state_code,
        locale,
        num_students,
        size,
        ia_bandwidth_per_student_kbps,
        ia_monthly_cost_total,
        peer_id,
        peer_name,
        peer_state_code,
        peer_locale,
        peer_num_students,
        peer_size,
        peer_ia_bw_mbps_total,
        peer_ia_monthly_cost_total
      from subset
      where fit_for_ia = true
      and meeting_2014_goal_no_oversub = false 
      and num_students > 9000

/*      select 
        district_id,
        name,
        state_code,
        locale,
        num_students,
        size,
        ia_bandwidth_per_student_kbps,
        ia_monthly_cost_total,
        count(distinct peer_id) as num_peers,
        outreach_status,
        engagement_status,
        account_owner

      from subset
      where fit_for_ia = true
      and meeting_2014_goal_no_oversub = false 
      and num_students > 9000
      group by 1,2,3,4,5,6,7,8,10,11,12

, evan_list as (
      select distinct on (district_id)
        district_id,
        name,
        state_code,
        locale,
        num_students,
        size,
        ia_bandwidth_per_student_kbps,
        case
          when peer_id is not null
            then true
          else false
        end as no_cost_peer_deal,
        ia_bw_mbps_total,
        case
          when fit_for_ia_17 = true
            then ia_bw_mbps_total_17
        end as ia_bw_mbps_total_17,
        case
          when fit_for_ia_16 = true
            then ia_bw_mbps_total_16
        end as ia_bw_mbps_total_16,
        case
          when fit_for_ia_15 = true
            then ia_bw_mbps_total_15
        end as ia_bw_mbps_total_15,
        ia_monthly_cost_per_mbps,
        year_of_last_upgrade,
        case
          when primary_sp = peer_primary_sp is null
            then false
          else primary_sp = peer_primary_sp 
        end as no_cost_peer_deal_from_same_sp,
        case
          when fit_for_ia_cost = true 
          and ia_annual_cost_erate != 0
          and peer_id is null 
            then (incr_cost_peer_ia_annual_cost_total*(ia_annual_cost_erate-ia_funding_requested_erate)/ia_annual_cost_erate) - 
                  case
                    when ia_funding_requested_erate > ia_annual_cost_total
                      then 0
                    else (ia_annual_cost_total-ia_funding_requested_erate) 
                  end
          when fit_for_ia_cost = true
          and peer_id is null
            then incr_cost_peer_ia_annual_cost_total*(1-c1_discount_rate)
        end as peer_oop_increase,
        case
          when fit_for_ia_cost = true 
          and ia_annual_cost_erate != 0
          and peer_id is null 
            then (ia_annual_cost_erate-ia_funding_requested_erate)/ia_annual_cost_erate
          when fit_for_ia_cost = true
          and peer_id is null
            then (1-c1_discount_rate)
        end as assumed_oop_rate,
        incr_cost_peer_id,
        incr_cost_peer_ia_annual_cost_total,
        case
          when fit_for_ia_cost = true 
          and ia_annual_cost_erate != 0
          and peer_id is null 
            then  case
                    when ia_funding_requested_erate > ia_annual_cost_total
                      then 0
                    else (ia_annual_cost_total-ia_funding_requested_erate) 
                  end
          when fit_for_ia_cost = true
          and peer_id is null
            then 0
        end as current_oop,
        fit_for_ia_cost,
        ia_annual_cost_erate,
        peer_id as no_cost_peer_id,
        c1_discount_rate
      from subset
      where fit_for_ia = true
      and meeting_2014_goal_no_oversub = false 
      and num_students > 9000
      order by district_id, primary_sp = peer_primary_sp desc
)

select
  district_id,
  name,
  state_code,
  locale,
  num_students,
  size,
  ia_bandwidth_per_student_kbps,
  no_cost_peer_deal,
  ia_bw_mbps_total,
  ia_monthly_cost_per_mbps,
  case
    when year_of_last_upgrade is null
    and (ia_bw_mbps_total_17-ia_bw_mbps_total_15)/ia_bw_mbps_total_15 >= .11
      then 2018
    when year_of_last_upgrade is null
    and ((ia_bw_mbps_total-ia_bw_mbps_total_16)/ia_bw_mbps_total_16 >= .11
    or (ia_bw_mbps_total-ia_bw_mbps_total_15)/ia_bw_mbps_total_15 >= .11)
      then 2019
    else year_of_last_upgrade
  end as year_of_last_upgrade,
  no_cost_peer_deal_from_same_sp,
  case
    when no_cost_peer_deal = false
    and peer_oop_increase is null
      then 'extrapolated'
    else peer_oop_increase::varchar
  end as peer_oop_increase,
  incr_cost_peer_id,
  incr_cost_peer_ia_annual_cost_total as peer_ia_annual_cost_total,
  assumed_oop_rate,
  current_oop,
  fit_for_ia_cost
from evan_list*/
