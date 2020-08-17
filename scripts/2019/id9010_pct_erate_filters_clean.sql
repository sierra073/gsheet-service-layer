with erate_filers as (
  select distinct
    dli.district_id,
    dfit.fit_for_ia,
    case when d.in_universe and d.district_type = 'Traditional' then true
    else false end as district_in_esh_universe

    from ps.districts_line_items dli

    join ps.districts_fit_for_analysis dfit
    on dli.district_id = dfit.district_id
    and dli.funding_year = dfit.funding_year

    join ps.districts d
    on dfit.district_id = d.district_id
    and dfit.funding_year = d.funding_year

    join ps.line_items li
    on dli.line_item_id = li.line_item_id
    and dli.funding_year = li.funding_year

    where dli.funding_year = 2019
    and li.erate = TRUE
    and li.bandwidth_in_mbps > 0
)

select 'all E-rate filers' as population,
count(case when fit_for_ia = true then district_id end)::numeric / count(*) as pct_erate_filters_clean
from erate_filers
group by 1

union

select 'ESH universe E-rate filers' as population,
count(case when fit_for_ia = true then district_id end)::numeric / count(*) as pct_erate_filters_clean
from erate_filers
where district_in_esh_universe = true
group by 1
