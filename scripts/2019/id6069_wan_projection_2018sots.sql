with circuit_costs as (
  select 
    limit_universe.state_code = 'AK' as state_ak,
    li.funding_year,
    li.bandwidth_in_mbps,
    count(distinct li.line_item_id) as line_items,
    avg(li.rec_cost/li.num_lines) as avg_rec_cost
  from ps.line_items li
  inner join (
    select  dli.line_item_id, d.state_code
    from ps.districts_line_items dli
    inner join ps.districts d
    on  dli.district_id = d.district_id
    and dli.funding_year = d.funding_year
    where d.district_type = 'Traditional'
    and d.in_universe = true 
    group by dli.line_item_id, d.state_code
  ) limit_universe 
  /*limiting universe without duplicating line items*/
  on limit_universe.line_item_id = li.line_item_id
  where li.funding_year in (2019,2018)
  and li.purpose = 'wan'
  and li.connect_category = 'Lit Fiber'
  and li.erate = true
  and li.dirty_labels = 0 
  and li.dirty_cost_labels = 0
  and li.exclude_labels = 0
  and li.bandwidth_in_mbps in (10000,1000)
  and li.rec_cost > 0 
  group by 1,2,3
),

subset as (
  select 
    dd.state_code = 'AK' as state_ak,
    dd.funding_year,
    dd.district_id,
    c.campus_id,
    dd.c1_discount_rate,
    sum(c.num_students) * 1.5 as wan_bw_needed,
    case
      when dd.num_campuses <= 1 
        then 0
      when sum(c.num_students) * 1.5 < 1000
        then 1000
      when sum(c.num_students) * 1.5 < 10000
        then 10000
      when sum(c.num_students) * 1.5 >= 10000
        then 20000
    end as wan_round_up_bw_needed
  --to filter for clean districts
  FROM ps.districts dd
  --to filter for Traditional districts in universe, and student quantification
  JOIN ps.campuses c
  ON c.district_id = dd.district_id
  AND c.funding_year = dd.funding_year
  where dd.funding_year in (2019,2018)
  and dd.district_type = 'Traditional'
  and dd.in_universe = true
  group by 1,2,3,4,5,dd.num_campuses
)

select 
  s.funding_year,
  sum( case
          when s.wan_round_up_bw_needed = 20000
            then 2*cc_2.avg_rec_cost*12
          when s.wan_round_up_bw_needed = 0
            then 0
          else cc.avg_rec_cost*12
        end) as wan_cost,
  sum( case
          when s.wan_round_up_bw_needed = 20000
            then 2*cc_2.avg_rec_cost*12
          when s.wan_round_up_bw_needed = 0
            then 0
          else cc.avg_rec_cost*12
        end*case
              when s.c1_discount_rate is null
                then .7
              else s.c1_discount_rate
            end) as wan_funding
from subset s
left join circuit_costs cc
on s.wan_round_up_bw_needed = cc.bandwidth_in_mbps
and s.funding_year = cc.funding_year
and s.state_ak = cc.state_ak
left join circuit_costs cc_2
on cc_2.bandwidth_in_mbps = 10000
and s.funding_year = cc_2.funding_year
and s.state_ak = cc_2.state_ak
group by 1
