with year_lkp as (
		select district_id, 
		       funding_year,
		       c2_received,
		       budget,
		       budget_post,
		       remaining,
		       remaining_post,
		       case 
		          when budget_post = remaining_post
		          then 0
		          when budget_post > remaining_post
		          then 1
		       end as year_started_lkp
		      
		from ps.districts_wifi 

		order by district_id

		),
		
		yr_lkp_min as (
		select 
		district_id,
		min(funding_year) as year_started
		
		from year_lkp
		where year_started_lkp = 1
		group by district_id ),
		
		num_years_funds_used as (
		
		
		select y.district_id, 
		       y.funding_year,
		       y.c2_received,
		       y.budget,
		       y.budget_post,
		       y.remaining,
		       y.remaining_post,
		      case 
		        when y.funding_year = y2.year_started 
		        then 1
		        when y.funding_year = y2.year_started + 1
		        then 2
		        when y.funding_year = y2.year_started + 2
		        then 3
		        when y.funding_year = y2.year_started + 3
		        then 4
		        else 0
		        end as num_years_funds_used
		      
		from year_lkp y
		
		left join yr_lkp_min y2
		on y.district_id = y2.district_id

		join ps.districts_fit_for_analysis dfa
		on y.district_id = dfa.district_id
		and y.funding_year = dfa.funding_year
		
		order by y.funding_year
 ),
 
agg as (
select d.state_code,
       d.funding_year,
       d.district_id,
       d.num_students,
       dw.budget_post,
       dw.remaining_post,
       y.num_years_funds_used,
       dw.remaining_post / dw.budget_post as perc_remaining_postdiscount
       
 
from ps.districts_wifi dw

left join ps.districts d
on dw.district_id = d.district_id
and dw.funding_year = d.funding_year

left join ps.districts_fit_for_analysis dffa
on d.district_id = dffa.district_id
and d.funding_year = dffa.funding_year

left join num_years_funds_used y
on d.district_id = y.district_id
and d.funding_year = y.funding_year

where d.district_type = 'Traditional'
and d.in_universe = true
and d.funding_year = 2019
-- and dw.remaining_post / dw.budget_post >= .5
and y.num_years_funds_used = 4)

select sum(remaining_post) as c2_funds_at_risk_expiring_in_2019
from agg
