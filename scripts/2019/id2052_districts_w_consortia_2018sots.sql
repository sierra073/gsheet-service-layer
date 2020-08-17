select
round(count(case 
		when consortium_affiliation is not null 
			then district_id
	end)::numeric/count(district_id),2) as districts_w_consortia_affiliation_p


from ps.districts  

where in_universe = true
and district_type = 'Traditional'
and funding_year = 2019
