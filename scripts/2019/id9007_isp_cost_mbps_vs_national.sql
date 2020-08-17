with districts_line_items as
(select distinct
  d.state_code,
  li.purpose,
  li.line_item_id,
  sum(li.total_monthly_cost) as total_monthly_cost,
  sum(li.bandwidth_in_mbps) as bandwidth_in_mbps
from ps.districts d
join ps.districts_fit_for_analysis dfit
on d.district_id = dfit.district_id
and d.funding_year = dfit.funding_year
join ps.districts_line_items dli
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year
join ps.line_items li
on dli.line_item_id = li.line_item_id
and dli.funding_year = li.funding_year

where d.funding_year = 2019
and d.in_universe = true
and d.district_type = 'Traditional'
and dfit.fit_for_ia = true
and li.consortium_shared = true
and li.purpose in ('isp','upstream')

group by 1,2,3
having sum(li.total_monthly_cost) > 0
)

select
  'WV' as subgroup,
  dli.purpose,
  median(dli.total_monthly_cost::numeric/dli.bandwidth_in_mbps) as median_monthly_cost_per_mbps
from districts_line_items dli
where dli.state_code = 'WV'
and bandwidth_in_mbps > 0
group by 1,2

union

select
  'KY' as subgroup,
  dli.purpose,
  median(dli.total_monthly_cost::numeric/dli.bandwidth_in_mbps) as median_monthly_cost_per_mbps
from districts_line_items dli
where dli.state_code = 'KY'
and bandwidth_in_mbps > 0
group by 1,2

union
select
  'RI' as subgroup,
  dli.purpose,
  median(dli.total_monthly_cost::numeric/dli.bandwidth_in_mbps) as median_monthly_cost_per_mbps
from districts_line_items dli
where dli.state_code = 'RI'
and bandwidth_in_mbps > 0
group by 1,2

union

select
  'National' as subgroup,
  dli.purpose,
  median(dli.total_monthly_cost::numeric/dli.bandwidth_in_mbps) as median_monthly_cost_per_mbps
from districts_line_items dli
where bandwidth_in_mbps > 0
group by 1,2
order by 1,2
