with subset as (
  select
    fit.district_id,
    dd.name,
    dd.locale,
    dd.num_students,
    dl.most_recent_ia_contract_end_date,
    bc.meeting_2014_goal_no_oversub,
    bc.ia_bandwidth_per_student_kbps,
    bc.ia_bw_mbps_total,
    bc.ia_monthly_cost_per_mbps,
    bc.ia_monthly_cost_total,
    bcpy.ia_monthly_cost_total as ia_monthly_cost_total_py,
    fit.fit_for_ia,
    fit.fit_for_ia_cost,
    fitpy.fit_for_ia as fit_for_ia_py,
    fitpy.fit_for_ia_cost as fit_for_ia_cost_py,
    d470.num_broadband_470s,
    up.upgrade_indicator,
    dl.ia_frns_received_zero_bids,
    --determined after discussing with jason and brian
    dl.fiber_internet_upstream_lines * 100000 +
    dl.fixed_wireless_internet_upstream_lines * 1000 +
    dl.cable_internet_upstream_lines * 1000 +
    dl.copper_internet_upstream_lines * 100 +
    dl.satellite_lte_internet_upstream_lines * 40 as bandwidth_maximum,
    (dd.num_students * .1) as bandwidth_needed_for_2014,
    sp.primary_sp,
    d470ay.district_id is null as no_470_indicator
  --to filter for clean districts
  FROM ps.districts_fit_for_analysis fit
  --to filter for Traditional districts in universe, and student quantification
  JOIN ps.districts dd
  ON fit.district_id = dd.district_id
  AND fit.funding_year = dd.funding_year
  --to determine if the district is meeting goals
  JOIN ps.districts_bw_cost bc
  ON fit.district_id = bc.district_id
  AND fit.funding_year = bc.funding_year
  --to determine if the district submitted a form 470 for broadband
  JOIN ps.districts_470s d470
  ON fit.district_id = d470.district_id
  AND fit.funding_year = d470.funding_year
  --to determine if the district submitted a form 471 for internet and received 0 bids
  JOIN ps.districts_lines dl
  ON fit.district_id = dl.district_id
  AND fit.funding_year = dl.funding_year
  --to determine if the district upgraded bw
  JOIN ps.districts_upgrades up
  ON fit.district_id = up.district_id
  AND fit.funding_year = up.funding_year
  --to determine if the district upgraded bw
  LEFT JOIN ps.districts_sp_assignments sp
  ON fit.district_id = sp.district_id
  AND fit.funding_year = sp.funding_year
  --to determine if the district was eligible for upgrade last year
  LEFT JOIN ps.districts_fit_for_analysis fitpy
  ON fit.district_id = fitpy.district_id
  AND fit.funding_year - 1 = fitpy.funding_year
  --to determine if the district spend more this year
  LEFT JOIN ps.districts_bw_cost bcpy
  ON fit.district_id = bcpy.district_id
  AND fit.funding_year - 1 = bcpy.funding_year
  --to determine if the district submitted a form 470 for broadband
  LEFT JOIN (
    select distinct district_id
    from ps.districts_470s
    where num_broadband_470s > 0
  ) d470ay
  ON fit.district_id = d470ay.district_id
  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
)
    select
      case
        when upgrade_indicator = true
          then 'upgraded but not enough'
        when num_broadband_470s = 0
          then 'not looking to upgrade'
        when ia_frns_received_zero_bids = 0
          then 'looking to upgrade but cant afford bids'
        else 'looking to upgrade but no bids'
      end as subgroup,
      *,
      bandwidth_maximum >= bandwidth_needed_for_2014 as could_meet_with_current_technology,
      case
        when fit_for_ia_cost = true
          then ia_monthly_cost_per_mbps
      end as ia_monthly_cost_per_mbps,
      case
        when fit_for_ia_cost = true
          then ia_monthly_cost_total
      end as ia_monthly_cost_total,
      case
        when fit_for_ia_cost = true
          then ia_monthly_cost_total::numeric/num_students
      end as ia_monthly_cost_per_student
    from subset
    where fit_for_ia = true
    and fit_for_ia_py = true
    and meeting_2014_goal_no_oversub = false
    order by 1