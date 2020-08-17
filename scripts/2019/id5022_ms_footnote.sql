with costs as (
select d.sn_weighted_average_cost_per_mbps as current_wtd_avg_cost_per_mb,
  a.elem as wtd_avg_cost_per_mb,
  a.nr + 2014 as funding_year

from dm.smd_state d

left join lateral unnest(sn_weighted_average_cost_per_mbps_all_years)
  WITH ORDINALITY AS a(elem, nr)
  ON TRUE

where state_code = 'MS'
and funding_year = 2019
and data_status = 'current'
),

cost_drop as (
select s18.median_ia_cost_per_mbps as median_cost_18,
  s19.median_ia_cost_per_mbps as median_cost_19,
  ((s18.median_ia_cost_per_mbps - s19.median_ia_cost_per_mbps) / s18.median_ia_cost_per_mbps)::numeric as perc_median_cost_drop,
  (select wtd_avg_cost_per_mb from costs where funding_year = 2018) as wtd_avg_cost_18,
  (select wtd_avg_cost_per_mb from costs where funding_year = 2019) as wtd_avg_cost_19,
  ((select wtd_avg_cost_per_mb from costs where funding_year = 2018) - (select wtd_avg_cost_per_mb from costs where funding_year = 2019))/(select wtd_avg_cost_per_mb from costs where funding_year = 2018)::numeric as perc_wtd_avg_cost_drop
from dm.smd_state s18

join dm.smd_state s19
on s19.state_code = s18.state_code
and s19.funding_year = 2019
and s19.data_status = 'current'

where s18.state_code = 'MS'
and s18.funding_year = 2018
),

districts as (
select bc.district_id,
  d.num_students,
  bc.ia_monthly_cost_per_mbps,
  (select perc_wtd_avg_cost_drop from cost_drop) as perc_drop,
  bc.ia_monthly_cost_per_mbps*(select 1-perc_wtd_avg_cost_drop from cost_drop) as projected_2023_cost_per_mb,
  bc.projected_bw_fy2018,
  bc.ia_monthly_cost_total,
  round(bc.projected_bw_fy2018*(bc.ia_monthly_cost_per_mbps*(select 1-perc_wtd_avg_cost_drop from cost_drop)),2 ) as projected_monthly_cost_2023

from ps.districts_bw_cost bc

join ps.districts d
on d.district_id = bc.district_id
and d.funding_year = bc.funding_year

join ps.districts_fit_for_analysis fit
on fit.district_id = bc.district_id
and fit.funding_year = bc.funding_year

where d.in_universe = True
and d.district_type = 'Traditional'
and fit.fit_for_ia = True
and fit.fit_for_ia_cost = True
and d.state_code = 'MS'
and d.funding_year = 2019
)

select round(perc_drop, 2) as perc_cost_decrease,
  count(distinct district_id) total_districts,
  count(case when ia_monthly_cost_total >= projected_monthly_cost_2023 THEN district_id else NULL end) as districts_will_meet,
  count(case when ia_monthly_cost_total < projected_monthly_cost_2023 THEN district_id else NULL end) as pay_more_districts,
  round(SUM(projected_monthly_cost_2023 - ia_monthly_cost_total) FILTER (Where ia_monthly_cost_total < projected_monthly_cost_2023), 2) as monthly_cost_pay_more_total,
  round(SUM(projected_monthly_cost_2023 - ia_monthly_cost_total) FILTER (Where ia_monthly_cost_total < projected_monthly_cost_2023)/count(case when ia_monthly_cost_total < projected_monthly_cost_2023 THEN district_id else NULL end), 2) as monthly_cost_pay_more_per_district,
  round(AVG(projected_monthly_cost_2023 - ia_monthly_cost_total) FILTER (Where ia_monthly_cost_total < projected_monthly_cost_2023), 2) as avg_cost_pay_more_per_district,
  round(MEDIAN(projected_monthly_cost_2023 - ia_monthly_cost_total) FILTER (Where ia_monthly_cost_total < projected_monthly_cost_2023), 2) as median_cost_pay_more_per_district,
  round(SUM(projected_monthly_cost_2023 - ia_monthly_cost_total) FILTER (Where ia_monthly_cost_total < projected_monthly_cost_2023)/SUM(case when ia_monthly_cost_total < projected_monthly_cost_2023 THEN num_students else 0 END ), 2) as cost_pay_more_per_student,
  round(sum(projected_monthly_cost_2023)/sum(projected_bw_fy2018)::numeric, 2) as projected_1mb_weighted_avg_cost_per_mb

from districts
group by perc_drop
