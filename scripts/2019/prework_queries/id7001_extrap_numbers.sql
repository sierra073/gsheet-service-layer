select dd.district_id,
dd.state_code,
case 
		when extract(month from  dl.most_recent_ia_contract_end_date) <= 6
			then extract(year from dl.most_recent_ia_contract_end_date)
		when extract(month from dl.most_recent_ia_contract_end_date) > 6
			then extract(year from dl.most_recent_ia_contract_end_date) + 1 
		else dd.funding_year + 1
	end as primary_new_contract_start_date,
case 
	when fit_for_ia = True
		and fit_for_ia_cost = True 
		and meeting_2018_goal_oversub = False 
		then 1
	else 0 
end as clean_path_1m_sample,
case 
	when fit_for_ia = True 
		and meeting_2018_goal_oversub = False
		then 1
	else 0
end as not_meeting,
case 
	when fit_for_ia = True 
		and meeting_2018_goal_oversub = True
		then 1
	else 0
end as meeting,
case
	when fit_for_ia = True
		then 1
	else 0
end as sample,
1 as population,
dd.num_students

from ps.districts dd 

inner join ps.districts_fit_for_analysis fit 
on fit.district_id = dd.district_id 
and fit.funding_year = dd.funding_year

inner join ps.districts_bw_cost bw 
on bw.district_id = dd.district_id 
and bw.funding_year = dd.funding_year 

inner join ps.districts_lines dl 
on dl.district_id = dd.district_id
and dl.funding_year = dd.funding_year

where dd.in_universe = true 
and dd.district_type = 'Traditional'
and dd.funding_year = 2019
