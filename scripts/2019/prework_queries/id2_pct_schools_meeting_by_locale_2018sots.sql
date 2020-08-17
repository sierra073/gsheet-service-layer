select 
    d.locale,
    sum(d.num_schools) as schools_meeting
from ps.districts_bw_cost dbc
join ps.districts d
on dbc.district_id = d.district_id
and dbc.funding_year = d.funding_year
join ps.districts_fit_for_analysis dffa
on dffa.district_id = d.district_id
and dffa.funding_year = d.funding_year
where d.funding_year = 2019
and d.in_universe = true
and dffa.fit_for_ia = true
and dbc.meeting_2014_goal_no_oversub = true
group by 1
