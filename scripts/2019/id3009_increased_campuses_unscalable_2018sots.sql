SELECT d.district_id,
      d.state_code,
      d.name,
      df.fiber_target_status as fiber_target_status_18,
      lydf.fiber_target_status as fiber_target_status_17,
      df.assumed_unscalable_campuses as assumed_unscalable_campuses_18,
      df.known_unscalable_campuses as known_unscalable_campuses_18,
      lydf.assumed_unscalable_campuses as assumed_unscalable_campuses_17,
      lydf.known_unscalable_campuses as known_unscalable_campuses_17,
      d.num_campuses,
      lyd.num_campuses as num_campuses_17
FROM ps.districts d

JOIN ps.districts_fiber df
on d.district_id = df.district_id
and d.funding_year = df.funding_year

join ps.districts_fiber lydf
on d.district_id = lydf.district_id
AND d.funding_year = lydf.funding_year + 1

join ps.districts lyd
on d.district_id = lyd.district_id
AND d.funding_year = lyd.funding_year + 1

WHERE df.assumed_unscalable_campuses + df.known_unscalable_campuses >
    lydf.assumed_unscalable_campuses + lydf.known_unscalable_campuses
    AND d.district_type = 'Traditional'
    AND d.in_universe = true
    and d.funding_year = 2019
    AND df.fiber_target_status = lydf.fiber_target_status
    AND d.num_campuses > lyd.num_campuses
