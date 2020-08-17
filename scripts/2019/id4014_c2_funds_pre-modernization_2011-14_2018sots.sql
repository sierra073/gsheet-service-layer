select 
-- cmtd_commitment_request is the amount received post discount in the funding request table
sum(cmtd_commitment_request::numeric) as cmtd_commitment_request

from ing.fy2015_funding_requests
where cmtd_category_of_service is not null
and cmtd_category_of_service not in ('VOICE SERVICES','INTERNET ACCESS', 'TELCOMM SERVICES')
and commitment_status != 'NOT FUNDED'
and application_type not in ('LIBRARY')
and funding_year::numeric <= 2014
and funding_year::numeric >= 2011