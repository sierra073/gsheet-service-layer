 
with pre_modernization as 
(with app_recip_lookup as (

select distinct 
       li.applicant_ben,
       dli.district_id as recipient_esh_id
       
from ps.districts_line_items dli

left join ps.line_items li
on li.line_item_id = dli.line_item_id
and li.funding_year=  dli.funding_year

left join ps.districts d
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and d.funding_year = 2019 
and li.erate = true
and li.exclude_labels = 0
and li.category_of_service = 2

UNION

select distinct 
      li.applicant_ben,
      dli.district_id as recipient_esh_id
       
from ps.line_items li

join ps.districts_line_items dli
on li.line_item_id = dli.line_item_id
and li.funding_year=  dli.funding_year

left join ps.districts d
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and d.funding_year = 2018 
and li.erate = true
and li.exclude_labels = 0
and li.category_of_service = 2

UNION

select distinct 
      li.applicant_ben,
      dli.district_id as recipient_esh_id
       
from ps.line_items li

join ps.districts_line_items dli
on li.line_item_id = dli.line_item_id
and li.funding_year=  dli.funding_year

left join ps.districts d
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and d.funding_year = 2017 
and li.erate = true
and li.exclude_labels = 0
and li.category_of_service = 2

UNION

select distinct 
      li.applicant_ben,
      dli.district_id as recipient_esh_id
       
from ps.line_items li

join ps.districts_line_items dli
on li.line_item_id = dli.line_item_id
and li.funding_year=  dli.funding_year

left join ps.districts d
on d.district_id = dli.district_id
and d.funding_year = dli.funding_year

where d.in_universe = true
and d.district_type = 'Traditional'
and d.funding_year = 2015 
and li.erate = true
and li.exclude_labels = 0
)

select 
1 as id,
count(distinct app_recip_lookup.recipient_esh_id) / (
	select count(district_id)::numeric
	from ps.districts
	where in_universe = true
	and district_type = 'Traditional'
	and funding_year = 2019
	) as perc_recipients

from ing.fy2015_funding_requests fr

left join app_recip_lookup
on fr.ben = app_recip_lookup.applicant_ben::varchar

where cmtd_category_of_service not in ('VOICE SERVICES','INTERNET ACCESS', 'TELCOMM SERVICES')
and cmtd_category_of_service is not null
and commitment_status != 'NOT FUNDED'
and application_type not in ('LIBRARY')
and fr.funding_year::numeric <= 2014
and fr.funding_year::numeric >= 2011),

post_modernization as (
select 1 as id, (count(distinct dw.district_id) filter (where dw.c2_received = true) / 
            count(distinct dw.district_id)::float) as perc_recipients


from ps.districts_wifi dw

join ps.districts d
on d.district_id = dw.district_id 
and d.funding_year = 2019

where d.district_type = 'Traditional'
and d.in_universe = true)

select post.perc_recipients / pre.perc_recipients as multiplier_growth_post_modernization_15_through_18

from pre_modernization pre

join post_modernization post
on pre.id = post.id



