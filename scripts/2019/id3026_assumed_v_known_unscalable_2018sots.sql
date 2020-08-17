SELECT SUM(fw.known_unscalable_campuses_fine_wine) AS known_unscalable,
      ROUND(SUM(fw.assumed_unscalable_campuses_fine_wine)) AS assumed_unscalable
FROM ps.smd_2019_fine_wine fw


WHERE fw.district_type = 'Traditional'
and fw.funding_year = 2019
