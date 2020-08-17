with peer_deal_sample as (select dd.district_id,
dd.num_students,
case
	when dl.most_recent_primary_ia_contract_end_date <= '2019-06-30'
		and dl.most_recent_primary_ia_contract_end_date > '2018-06-30'
		then true
	when dl.most_recent_primary_ia_contract_end_date > '2019-06-30'
		then false
end as contract_expiring_primary,
u.path_to_meet_2018_goal_group = 'No Cost Peer Deal' as line_item_deal

from ps.districts dd

inner join ps.districts_fit_for_analysis fit
on dd.district_id = fit.district_id
and dd.funding_year = fit.funding_year

inner join ps.districts_bw_cost bc
on dd.district_id = bc.district_id
and dd.funding_year = bc.funding_year

left join ps.districts_lines dl
on dd.district_id = dl.district_id
and dd.funding_year = dl.funding_year

inner join ps.districts_upgrades u
on dd.district_id = u.district_id
and dd.funding_year = u.funding_year

inner join ps.states_static s
on dd.state_code = s.state_code

where dd.funding_year = 2018
and dd.district_type = 'Traditional'
and dd.in_universe = True
and fit.fit_for_ia = True
and fit.fit_for_ia_cost = True
and bc.ia_monthly_cost_total > 0
and bc.meeting_2018_goal_oversub = False
and dl.most_recent_primary_ia_contract_end_date is not null
and s.state_network = false
),

extrap_pop as (
		select round((count(case
								when fit.fit_for_ia= True and bw.meeting_2018_goal_oversub = False
									then d.district_id
								end)::numeric/
						count(case
								when fit.fit_for_ia = True
									then d.district_id
								end))
					*count(d.district_id)) as extrapolated_districts_not_meeting,
						round((sum(case
								when fit.fit_for_ia= True and bw.meeting_2018_goal_oversub = False
									then d.num_students
								end)::numeric/
							sum(case
								when fit.fit_for_ia = True
									then d.num_students
								end))
						*sum(d.num_students)) as extrapolated_students_not_meeting

		from ps.districts_fit_for_analysis fit

		inner join ps.districts d
		on d.district_id = fit.district_id
		and d.funding_year = fit.funding_year

		inner join ps.districts_bw_cost bw
		on bw.district_id = fit.district_id
		and bw.funding_year = fit.funding_year

		inner join ps.states_static s
		on d.state_code = s.state_code

		where d.funding_year = 2018
		and d.district_type = 'Traditional'
		and d.in_universe = true
		and s.state_network = False

)

select
round((count(case
	when d.contract_expiring_primary = True and d.line_item_deal = True
		then d.district_id end)::numeric/count(d.district_id))*extrapolated_districts_not_meeting) as districts,
round(((sum(case
	when d.contract_expiring_primary = True and d.line_item_deal = True
		then d.num_students end)::numeric/sum(d.num_students))*extrapolated_students_not_meeting)/1000000,2) as million_students

from peer_deal_sample d

join extrap_pop
on TRUE

group by extrapolated_districts_not_meeting, extrapolated_students_not_meeting
