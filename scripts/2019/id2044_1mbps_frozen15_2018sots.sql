select 
count(case
	when f.fit_for_ia = true and bw.meeting_2019_goal_no_oversub = true 
		then d.district_id end)::numeric/count(case
			when f.fit_for_ia = true 
				then d.district_id end)
as districts_p,
round(count(case
	when f.fit_for_ia = true and bw.meeting_2019_goal_no_oversub = true 
		then d.district_id end)::numeric/count(case
			when f.fit_for_ia = true 
				then d.district_id end)*count(d.district_id))
as districts,
sum(case
	when f.fit_for_ia = true and bw.meeting_2019_goal_no_oversub = true 
		then d.num_students end)::numeric/sum(case
			when f.fit_for_ia = true 
				then d.num_students end)
as students_p,
round(sum(case
	when f.fit_for_ia = true and bw.meeting_2019_goal_no_oversub = true 
		then d.num_students end)::numeric/sum(case
			when f.fit_for_ia = true 
				then d.num_students end)*sum(d.num_students))
as students

from ps.districts_fit_for_analysis_frozen_sots  f

join (select bw.funding_year,
bw.district_id,
case
	when d.size in ('Tiny','Small')
		then bw.ia_bandwidth_per_student_kbps >= 1000
	when d.size in ('Medium','Large','Mega')
		then bw.ia_bandwidth_per_student_kbps >= 700
end as meeting_2019_goal_no_oversub

from ps.districts_bw_cost_frozen_sots bw

join ps.districts_frozen_sots d 
on bw.district_id = d.district_id 
and bw.funding_year = d.funding_year
) bw 
on bw.district_id = f.district_id 
and bw.funding_year = f.funding_year

join ps.districts_frozen_sots d 
on d.district_id = f.district_id
and d.funding_year = f.funding_year 

where f.funding_year = 2015

and d.district_type = 'Traditional'