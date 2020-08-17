select 
dd.funding_year,
dd.state_code,
dd.district_id,
dd.num_students,
bc.ia_bw_mbps_total,
bc.ia_monthly_cost_total,
bc.ia_bandwidth_per_student_kbps,
bc.projected_bw_fy2018

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
on s.state_code = dd.state_code

where dd.funding_year = 2019
and dd.district_type = 'Traditional'
and dd.in_universe = True 
and fit.fit_for_ia = True
and fit.fit_for_ia_cost = True 
and bc.ia_monthly_cost_total > 0
and bc.meeting_2018_goal_oversub = False
and u.path_to_meet_2018_goal_group != 'No Cost Peer Deal'
and s.peer_deal_type = 'line_items'