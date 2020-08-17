WITH states AS (SELECT fw.funding_year,
      fw.state_code,
      ROUND(1-(SUM(CASE WHEN fw.funding_year = 2019 THEN (fw.assumed_unscalable_campuses_fine_wine + fw.known_unscalable_campuses_fine_wine)
          ELSE (fw.assumed_unscalable_campuses + fw.known_unscalable_campuses) END)/Sum(fw.num_campuses)),2) AS percent_scalable_campuses


FROM ps.smd_2019_fine_wine fw


WHERE fw.district_type = 'Traditional'
AND fw.funding_year = 2019
AND fw.state_code != 'DC'
AND fw.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                    'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')

GROUP BY fw.funding_year,
      fw.state_code

Having ROUND(1-(SUM(CASE WHEN fw.funding_year = 2019 THEN (fw.assumed_unscalable_campuses_fine_wine + fw.known_unscalable_campuses_fine_wine)
          ELSE (fw.assumed_unscalable_campuses + fw.known_unscalable_campuses) END)/Sum(fw.num_campuses)),2) >= .99

)

SELECT COUNT(*)
FROM states
