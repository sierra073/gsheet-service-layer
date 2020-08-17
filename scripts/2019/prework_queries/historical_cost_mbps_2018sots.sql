with pct_fr_internet_in_universe as (
  select 
    sum(commitment_amount_request) FILTER (WHERE category_of_spend = 'Internet'
                                              and in_universe = true)/
      sum(commitment_amount_request) as pct_fr_internet_in_universe
  from dm.cost_summary 
  where category_of_spend not in ('Cat 2', 'Voice', 'Special Construction')
),

ia_funding as (
  select 
    funding_year,
    round(sum(case
                when cmtd_category_of_service is null and orig_category_of_service = 'INTERNET ACCESS'
                  then orig_commitment_request::numeric
                when cmtd_category_of_service = 'INTERNET ACCESS'
                  then cmtd_commitment_request::numeric
                else 0
              end)*pct_fr_internet_in_universe,-6) as ia_funding,
    round(sum(case
                when cmtd_category_of_service is null
                  then orig_commitment_request::numeric
                else cmtd_commitment_request::numeric
              end),-6) as overall_funding
  from ing.fy2015_funding_requests
  join pct_fr_internet_in_universe
  on true
  where funding_year = 2013
  group by 1, pct_fr_internet_in_universe
    
    UNION
    
  select 
    funding_year,
    round(sum(case
                when category_of_spend = 'Internet'
                and in_universe = true
                  then commitment_amount_request
                else 0
              end),-6) as ia_funding,
    round(sum(commitment_amount_request),-6) as overall_funding
              
  from dm.cost_summary 
  group by 1
),

ia_bw_per_student as (
  select
    funding_year,
    round(median(ia_bandwidth_per_student_kbps),0) as median_ia_bandwidth_per_student_kbps
  from ps.smd_2019_fine_wine
  where fit_for_ia = true
  group by 1
    UNION
  select
    2013 as funding_year,
    52  as median_ia_bandwidth_per_student_kbps
),

ia_funding_per_mbps as (
  select
    funding_year,
    round(sum(ia_monthly_cost_total)/sum(ia_bw_mbps_total),2) as wtavg_ia_monthly_cost_per_mbps
  from ps.smd_2019_fine_wine
  where fit_for_ia_cost = true
  group by 1
    UNION
  select
    2013 as funding_year,
    10.60  as median_ia_bandwidth_per_student_kbps
)

select 
  ia_bw_per_student.funding_year,
  ia_bw_per_student.median_ia_bandwidth_per_student_kbps,
  ia_funding.ia_funding,
  ia_funding.overall_funding,
  ia_funding_per_mbps.wtavg_ia_monthly_cost_per_mbps
from ia_bw_per_student
join ia_funding
on ia_bw_per_student.funding_year = ia_funding.funding_year
join ia_funding_per_mbps
on ia_bw_per_student.funding_year = ia_funding_per_mbps.funding_year
order by ia_bw_per_student.funding_year
