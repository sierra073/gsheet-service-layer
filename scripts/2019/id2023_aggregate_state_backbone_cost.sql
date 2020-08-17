with only_districts as (
select distinct
li.funding_year,
applicant_state,
applicant_name, -- UT, AR, ND, DE, NE, WY
applicant_ben,
sum(dli.total_monthly_cost) filter(where exclude_labels = 0) as districts_total_backbone_cost

from ps.line_items li

left join ps.districts_line_items dli
on li.line_item_id = dli.line_item_id
and li.funding_year = dli.funding_year

left join ps.districts d
on dli.district_id = d.district_id
and dli.funding_year = d.funding_year
and d.in_universe

where applicant_ben in (157441,198892,157107,153062,16074952,16055092,154332,225870,120839,134238,150246,152695,16071127) --hard-coded list of state network BENs for relevant states
and li.funding_year = 2019
and li.purpose = 'backbone'
and dirty_cost_labels = 0
and dirty_labels = 0

group by 1,2,3,4)

SELECT
li.applicant_state,
li.applicant_name, -- UT, AR, ND, DE, NE, WY
li.applicant_ben,
round(districts_total_backbone_cost,2) as districts_total_backbone_cost,
round(sum(li.total_monthly_cost),2) as state_total_backbone_cost,
CASE
  WHEN districts_total_backbone_cost is not NULL
    then round((sum(li.total_monthly_cost) - districts_total_backbone_cost),2)
  ELSE round(sum(li.total_monthly_cost),2)
END as unalloc_or_excluded_cost

from only_districts od

join ps.line_items li
on od.applicant_ben = li.applicant_ben
and od.funding_year = li.funding_year

where li.funding_year = 2019
and li.purpose = 'backbone'
and dirty_cost_labels = 0
and dirty_labels = 0

group by
li.applicant_state,
li.applicant_name,
li.applicant_ben,
districts_total_backbone_cost
