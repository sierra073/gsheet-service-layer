with subset as (
  select 
    d.district_id,
    d.name,
    d.state_code,
    d.locale,
    d.num_students,
    dl.most_recent_ia_contract_end_date as most_recent_ia_contract_end_date_cy,
    up.upgrade_indicator,
    dbc.ia_bandwidth_per_student_kbps,
    dbc.ia_bw_mbps_total,
    dbc.ia_monthly_cost_total,
    sp.primary_sp,
    d.outreach_status,
    d.engagement_status,
    d.account_owner,
    dfa.fit_for_ia,
    dbc.meeting_2014_goal_no_oversub,
    d470.num_broadband_470s,
    dlpy.most_recent_ia_contract_end_date as most_recent_ia_contract_end_date_py,
    dl.ia_frns_received_zero_bids

  from ps.districts d

  left join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year

  left join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year

  left JOIN ps.districts_470s d470
  ON d.district_id = d470.district_id
  AND d.funding_year = d470.funding_year

  left JOIN ps.districts_lines dl
  ON d.district_id = dl.district_id
  AND d.funding_year = dl.funding_year

  left JOIN ps.districts_lines dlpy
  ON d.district_id = dlpy.district_id
  AND d.funding_year-1 = dlpy.funding_year

  left JOIN ps.districts_upgrades up
  ON d.district_id = up.district_id
  AND d.funding_year = up.funding_year

  LEFT JOIN ps.districts_sp_assignments sp
  ON d.district_id = sp.district_id
  AND d.funding_year = sp.funding_year

  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
)

select *
from subset
where meeting_2014_goal_no_oversub = false
and fit_for_ia = true
and num_broadband_470s > 0
--only need to review districts that had expiring contracts last year
and most_recent_ia_contract_end_date_py <= '2019-06-30'::date
--only need to review districts that got 470 bids
and ia_frns_received_zero_bids = 0
