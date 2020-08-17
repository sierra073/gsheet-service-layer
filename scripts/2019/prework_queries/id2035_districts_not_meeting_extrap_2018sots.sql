select 
count(
	case 
		when fit.fit_for_ia = true and bw.meeting_2018_goal_oversub = false 
			then d.district_id end)::numeric/
	count(case 
			when fit.fit_for_ia = true 
				then d.district_id end)
as districts_not_meeting_p,
round(count(
	case 
		when fit.fit_for_ia = true and bw.meeting_2018_goal_oversub = false 
			then d.district_id end)::numeric/
	count(case 
			when fit.fit_for_ia = true 
				then d.district_id end)*count(d.district_id))
as districts_not_meeting_extrap,

sum(
	case 
		when fit.fit_for_ia = true and bw.meeting_2018_goal_oversub = false 
			then d.num_students end)::numeric/
	sum(case 
			when fit.fit_for_ia = true 
				then d.num_students end)
as students_not_meeting_p,
round(sum(
	case 
		when fit.fit_for_ia = true and bw.meeting_2018_goal_oversub = false 
			then d.num_students end)::numeric/
	sum(case 
			when fit.fit_for_ia = true 
				then d.num_students end)*sum(num_students))
as students_not_meeting_extrap

from ps.districts_fit_for_analysis fit 

inner join ps.districts d
on d.district_id = fit.district_id
and d.funding_year = fit.funding_year

inner join ps.districts_bw_cost bw 
on bw.district_id = fit.district_id
and bw.funding_year = fit.funding_year


where d.funding_year = 2019
and d.district_type = 'Traditional'
and d.in_universe = true 