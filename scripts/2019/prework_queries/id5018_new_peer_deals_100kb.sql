with peer_deal_agg as (
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
  END as all_current_provider_deal

from ps.peer_deal_line_items

group by funding_year,
  district_id,
  deal
),

subset as (
  select
    fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bc.ia_bandwidth_per_student_kbps,
    bc.ia_bw_mbps_total,
    bc.ia_monthly_cost_per_mbps,
    bc.ia_monthly_cost_total,
    fit.fit_for_ia,
    fit.fit_for_ia_cost,
    dd.num_students,
    count(distinct p.deal) AS num_peer_deals,
    sum(p.all_current_provider_deal) as num_current_provider,
    sum(all_esh_deal) as num_esh_only_deals,
    sum(any_geotel_deal) as num_geotel_deals
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

  --to determine if the district has a peer deal for bandwidth
  LEFT JOIN peer_deal_agg p
  ON fit.district_id = p.district_id
  AND fit.funding_year = p.funding_year

  JOIN ps.states_static ss
  on ss.state_code = dd.state_code

  where fit.funding_year = 2019
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
  and ss.peer_deal_type = 'line_items'
  and fit.fit_for_ia = True
  and fit.fit_for_ia_cost = True
  and bc.ia_monthly_cost_total > 0
  and bc.meeting_2014_goal_no_oversub = False

  group by fit.district_id,
    bc.meeting_2014_goal_no_oversub,
    bc.ia_bandwidth_per_student_kbps,
    bc.ia_bw_mbps_total,
    bc.ia_monthly_cost_per_mbps,
    bc.ia_monthly_cost_total,
    fit.fit_for_ia,
    fit.fit_for_ia_cost,
    dd.num_students
),

extrap_pop as (
    select round((count(case
                when fit.fit_for_ia= True and bw.meeting_2014_goal_no_oversub = False
                  then d.district_id
                end)::numeric/
            count(case
                when fit.fit_for_ia = True
                  then d.district_id
                end))
          *count(d.district_id)) as extrapolated_districts_not_meeting,
            round((sum(case
                when fit.fit_for_ia= True and bw.meeting_2014_goal_no_oversub = False
                  then d.num_students
                end)::numeric/
              sum(case
                when fit.fit_for_ia = True
                  then d.num_students
                end))
            *sum(d.num_students)) as extrapolated_students_not_meeting

    from ps.districts_fit_for_analysis fit

    inner join ps.districts d
    on d.district_id = fit.district_id
    and d.funding_year = fit.funding_year

    inner join ps.districts_bw_cost bw
    on bw.district_id = fit.district_id
    and bw.funding_year = fit.funding_year

    inner join ps.states_static s
    on d.state_code = s.state_code

    where d.funding_year = 2019
    and d.district_type = 'Traditional'
    and d.in_universe = true
    and s.peer_deal_type = 'line_items'

)

select
  extrapolated_districts_not_meeting,
  extrapolated_students_not_meeting,
  round((count(district_id) filter (where num_peer_deals > 0)::numeric/count(district_id))*extrapolated_districts_not_meeting) as districts_with_peers,
  round((sum(num_students) filter (where num_peer_deals > 0)::numeric/sum(num_students))*extrapolated_students_not_meeting) as students_with_peers,
  round((count(district_id) filter (where num_peer_deals = 0)::numeric/count(district_id))*extrapolated_districts_not_meeting) as districts_no_peers,
  round((sum(num_students) filter (where num_peer_deals = 0)::numeric/sum(num_students))*extrapolated_students_not_meeting) as students_no_peers,
  round((count(district_id) filter (where num_esh_only_deals = num_peer_deals and num_peer_deals > 0)::numeric/count(district_id))*extrapolated_districts_not_meeting) as districts_esh_peers_only,
  round((sum(num_students) filter (where num_esh_only_deals = num_peer_deals and num_peer_deals > 0)::numeric/sum(num_students))*extrapolated_students_not_meeting) as students_esh_peers_only,
  round((count(district_id) filter (where num_geotel_deals > 0)::numeric/count(district_id))*extrapolated_districts_not_meeting) as districts_geotel_peers,
  round((sum(num_students) filter (where num_geotel_deals > 0)::numeric/sum(num_students))*extrapolated_students_not_meeting) as students_geotel_peers,
  round((count(district_id) filter (where num_geotel_deals = num_peer_deals and num_geotel_deals > 0)::numeric/count(district_id))*extrapolated_districts_not_meeting) as districts_geotel_peers_only,
  round((sum(num_students) filter (where num_geotel_deals = num_peer_deals and num_geotel_deals > 0)::numeric/sum(num_students))*extrapolated_students_not_meeting) as students_geotel_peers_only

from subset

join extrap_pop
on TRUE

group by extrapolated_districts_not_meeting,
extrapolated_students_not_meeting
