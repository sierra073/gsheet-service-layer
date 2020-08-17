select 
    d.district_id,
    assumed_unscalable_campuses + known_unscalable_campuses as num_unscalable_campuses
from ps.districts_fiber df
join ps.districts d
on df.district_id = d.district_id
and df.funding_year = d.funding_year
where d.funding_year = 2019
and d.in_universe = true
and assumed_unscalable_campuses + known_unscalable_campuses > 0
