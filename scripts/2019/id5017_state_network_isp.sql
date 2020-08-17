with recipients as (
  select d.state_code,
    count(distinct dli.district_id) as district_isp_recipients,
    count(distinct dli.line_item_id) as isp_lines_received

  from ps.districts_line_items dli

  join ps.districts d
  on d.district_id = dli.district_id
  and d.funding_year = dli.funding_year

  where dli.funding_year = 2019
  and dli.purpose = 'isp'
  and d.in_universe = True
  and d.district_type = 'Traditional'

  group by d.state_code
)

select applicant_state,
  round(sum(bandwidth_in_mbps)/studs.total_students*1000) as bw_per_student_kbps,
  round(r.district_isp_recipients/studs.num_districts::numeric, 2) as perc_districts_receiving

from ps.line_items li

join ps.states_static ss
on ss.state_code = li.applicant_state

join (select
  d.state_code,
  sum(d.num_students) as total_students,
  count(distinct d.district_id) as num_districts
  from ps.districts d
  where d.funding_year = 2019
  and in_universe = True
  and district_type = 'Traditional'
  group by 1) studs on studs.state_code = li.applicant_state

join recipients r
on r.state_code = ss.state_code

where li.funding_year = 2019
and purpose = 'isp'
and exclude_labels = 0
and dirty_labels = 0
and ss.state_network = True
and (r.district_isp_recipients/studs.num_districts::numeric) > .5

group by applicant_state, ss.procurement,
  studs.total_students, studs.num_districts,
  r.district_isp_recipients,
  r.isp_lines_received
order by bw_per_student_kbps desc
