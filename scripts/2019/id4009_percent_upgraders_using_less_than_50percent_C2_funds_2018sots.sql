with agg as (

select d.state_code,
       d.funding_year,
       d.district_id,
       dw.budget_post,
       dw.remaining_post,
       dw.budget_allocated_post,
       dw.remaining_post / dw.budget_post as perc_remaining_postdiscount,
       case
        when du18.upgrade_indicator = TRUE then TRUE
        when du17.upgrade_indicator = TRUE then TRUE
        when du16.upgrade_indicator = TRUE then TRUE
        else FALSE
      end as upgrade_indicator,
      dbc.meeting_2014_goal_no_oversub,
      dffa.fit_for_ia
       
 
from ps.districts_wifi dw

left join ps.districts d
on dw.district_id = d.district_id
and dw.funding_year = d.funding_year

left join ps.districts_fit_for_analysis dffa
on d.district_id = dffa.district_id
and d.funding_year = dffa.funding_year

left join ps.districts d17
on d17.district_id = d.district_id

left join ps.districts d16
on d16.district_id = d.district_id

left join ps.districts_upgrades du18
on du18.district_id = d.district_id

left join ps.districts_upgrades du17
on du17.district_id = d.district_id

left join ps.districts_upgrades du16
on du16.district_id = d.district_id

left join ps.districts_bw_cost dbc
on dw.district_id = dbc.district_id
and dw.funding_year = dbc.funding_year 

where d.district_type = 'Traditional'
and d.in_universe = true
and d17.in_universe = true
and d16.in_universe = true
and d.funding_year = 2019
and d17.funding_year = 2018
and d16.funding_year = 2017
and du18.funding_year = 2019
and du17.funding_year = 2018
and du16.funding_year = 2017)

select count(district_id) filter (where upgrade_indicator = true 
                                  and meeting_2014_goal_no_oversub = true
                                  and fit_for_ia = true
                                  and perc_remaining_postdiscount >= .5)::float /
                                  count(district_id) filter (where upgrade_indicator = true 
                                  and meeting_2014_goal_no_oversub = true
                                  and fit_for_ia = true)::float
                                  as percent_upgraders_using_less_than_50percent_C2_funds
from agg