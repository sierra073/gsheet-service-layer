SELECT
      ROUND(SUM(CASE WHEN d.locale IN ('Rural','Town')
        THEN fw.known_unscalable_campuses_fine_wine + fw.assumed_unscalable_campuses_fine_wine
        END) / SUM(fw.known_unscalable_campuses_fine_wine + fw.assumed_unscalable_campuses_fine_wine)
        ,4) AS percent_rural_unscalable_campuses
FROM ps.districts d

JOIN ps.smd_2019_fine_wine  fw
ON d.district_id = fw.district_id
AND d.funding_year = fw.funding_year

WHERE (fw.known_unscalable_campuses_fine_wine + fw.assumed_unscalable_campuses_fine_wine) > 0
AND d.funding_year = 2019
and d.in_universe = true
and d.district_type = 'Traditional'
