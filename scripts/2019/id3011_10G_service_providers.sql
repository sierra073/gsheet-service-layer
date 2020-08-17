select d.state_code,
d.funding_year,
count(distinct li.service_provider_id)

from ps.districts d

join ps.entity_bens_lkp ebl
on d.district_id=ebl.entity_id
and d.funding_year=ebl.funding_year

join ps.line_items li
on li.applicant_ben=ebl.ben
and d.funding_year=li.funding_year

where li.purpose = 'wan'
and li.bandwidth_in_mbps = 10000
and d.state_code not in ('AL','CT','DE','GA','HI','KY','ME','MO','MS','NC','ND','NE','RI','SD','SC','TN','UT','WA','WV','WY')
--and li.funding_year = 2015

group by d.state_code,
  d.funding_year
order by d.state_code