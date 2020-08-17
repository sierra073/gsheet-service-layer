with final_districts as (
select
  d.district_id,
  MIN(d.funding_year) as first_year_meeting,
  sum(b.meeting_2014_goal_no_oversub::int) as num_years_meeting_since_first,
  sum(u.upgrade_indicator::int) as num_years_upgraded,
  sum( CASE WHEN d.funding_year != (select MIN(bb.funding_year)
                                    from ps.districts_bw_cost bb
                                    where bb.meeting_2014_goal_no_oversub=True
                                    and bb.district_id=d.district_id)
            THEN u.upgrade_indicator::int END)
    as num_years_upgraded_excl_first,
  count(d.funding_year) as num_clean_years

from ps.districts d

join ps.districts_bw_cost b
on b.district_id = d.district_id
and b.funding_year = d.funding_year

join ps.districts_fit_for_analysis ffa
on ffa.district_id = d.district_id
and ffa.funding_year = d.funding_year

join ps.districts_upgrades u
on u.district_id = d.district_id
and u.funding_year = d.funding_year

where ffa.fit_for_ia = true
and d.in_universe = True
and d.district_type = 'Traditional'
-- only include districts that are currently meeting
and d.district_id in (select district_id from ps.districts_bw_cost where funding_year = 2019 and meeting_2014_goal_no_oversub = True)
and b.meeting_2014_goal_no_oversub = True

group by d.district_id
)

select count(distinct district_id) as total_districts_meeting,
  count(district_id) FILTER (WHERE num_years_upgraded_excl_first >= 1) as total_upgraded_since_meeting,
  round(count(district_id) FILTER (WHERE num_years_upgraded_excl_first >= 1)/count(district_id)::decimal*100, 1) as percent_upgraded_since_meeting,
  count(district_id) FILTER (WHERE num_years_upgraded_excl_first > 1) as total_upgraded_many_since_meeting,
  round(count(district_id) FILTER (WHERE num_years_upgraded_excl_first > 1)/count(district_id)::decimal*100, 1) as percent_upgraded_many_since_meeting
from final_districts
