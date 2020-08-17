--from https://sites.google.com/educationsuperhighway.org/sots-metrics-2018/home
with temp as (


select sum(funding_commitment_request::numeric) a

from fy2017.frns 

where frn not in (

  select frn

  from fy2017.current_frns)


UNION


select sum(funding_commitment_request::numeric) a

from fy2017.current_frns)


select sum(a)

from temp 