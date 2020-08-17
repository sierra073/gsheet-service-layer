SELECT
      ROUND(SUM(fw.known_unscalable_campuses_fine_wine + fw.assumed_unscalable_campuses_fine_wine))
FROM ps.smd_2019_fine_wine fw

WHERE  fw.district_type = 'Traditional'
and fw.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI')


      
