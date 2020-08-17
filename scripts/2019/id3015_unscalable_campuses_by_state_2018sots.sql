SELECT fw.state_code,
         CASE WHEN fw.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')
            THEN true
            ELSE false
      END AS state_match,
      ROUND(SUM(fw.assumed_unscalable_campuses_fine_wine + fw.known_unscalable_campuses_fine_wine))
      AS unscalable_campuses
FROM ps.smd_2019_fine_wine fw

WHERE fw.funding_year = 2019
AND fw.district_type = 'Traditional'
AND state_code != 'DC'

GROUP BY fw.state_code
