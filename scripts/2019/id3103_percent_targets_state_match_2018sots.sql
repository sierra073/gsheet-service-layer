SELECT COUNT(CASE WHEN fw.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME',
                          'MO','MT','NC','NH','NM','NV','NY','OK', 'OR','TX','VA','WA','WI') THEN 1 else null END)::numeric
        / COUNT(*)::numeric AS percent_targets_in_state_match

FROM ps.smd_2019_fine_wine fw


WHERE fw.district_type = 'Traditional'
AND fw.funding_year = 2019
AND fw.fiber_target_status = 'Target'
