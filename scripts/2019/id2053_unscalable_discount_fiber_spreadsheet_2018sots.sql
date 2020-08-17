select 
d.funding_year,
d.state_code,
d.state_code in ('AZ','CA','CO','FL','ID','IL','KS','MA','MD','ME','MO','MT','NC','NH','NM','NV','NY','OK','OR','TX','VA','WA','WI')
as state_match_states,
d.state_code in ('AZ','IL','MO','WA')
as full_cost_build_states,
d.district_id,
d.name,
--fiber
f.known_unscalable_campuses + f.assumed_unscalable_campuses
as total_unscalable_campuses,
d.c1_discount_rate



from ps.districts_fit_for_analysis fit 

inner join ps.districts d
on d.district_id = fit.district_id
and d.funding_year = fit.funding_year

inner join ps.districts_fiber f 
on f.district_id = fit.district_id
and f.funding_year = fit.funding_year

where d.district_type = 'Traditional'
and d.in_universe = true 
and d.funding_year = 2019
and (f.known_unscalable_campuses + f.assumed_unscalable_campuses) > 0
