select d.funding_year,
  sum(d.num_teachers)

from ps.districts d

--join ps.districts_fit_for_analysis dffa
--on dffa.district_id = d.district_id
--and dffa.funding_year = d.funding_year

where d.funding_year in (2019, 2018)
and d.district_type = 'Traditional'
and d.in_universe = true
--and dffa.fit_for_ia = true

group by d.funding_year