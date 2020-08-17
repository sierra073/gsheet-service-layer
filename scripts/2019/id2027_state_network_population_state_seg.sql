SELECT
aa.state_code,
num_dist_in_netwk,
num_dist_in_state,
perc_dist_in_netwk,
sum(CASE
      WHEN d.district_id = any(list_of_dists)
        then d.num_students
    END) as num_students_in_netwk,
sum(CASE
      WHEN d.district_id = any(list_of_dists)
        then d.num_schools
    END) as num_schools_in_netwk

from ps.districts d

join
    (SELECT
    ssd.state_code,
    count(distinct CASE
                    WHEN consortia_internet_applicants ilike state_network
                    OR consortia_isp_applicants ilike state_network
                    OR consortia_upstream_applicants ilike state_network
                    OR consortia_wan_applicants ilike state_network
                      then ssd.district_id
                   END) as num_dist_in_netwk,
    count(distinct ssd.district_id) as num_dist_in_state,
    count(distinct CASE
                    WHEN consortia_internet_applicants ilike state_network
                    OR consortia_isp_applicants ilike state_network
                    OR consortia_upstream_applicants ilike state_network
                    OR consortia_wan_applicants ilike state_network
                      then ssd.district_id
                   END)::numeric/count(distinct ssd.district_id)::numeric as perc_dist_in_netwk,
    array_agg(distinct CASE
                        WHEN consortia_internet_applicants ilike state_network
                        OR consortia_isp_applicants ilike state_network
                        OR consortia_upstream_applicants ilike state_network
                        OR consortia_wan_applicants ilike state_network
                          then ssd.district_id
                       END) as list_of_dists

    from dm.state_specific_dash_districts ssd

    join
    (SELECT
    state_code,
    mode() within group (order by applicant_name) as state_network

    from ps.line_items li

    join ps.districts_line_items dli
    on li.line_item_id = dli.line_item_id
    and li.funding_year = dli.funding_year

    left join ps.districts d
    on dli.district_id = d.district_id
    and dli.funding_year = d.funding_year

    where li.funding_year = 2019
    and state_code in ('AL','AR','DE','GA','IA','KY','ME','MO','MS','NC','ND','NE','RI','SC','SD','UT','WA','WI','WV','WY')

    group by 1) sn
    on ssd.state_code = sn.state_code

    group by 1

    UNION

    SELECT
    state_code,
    count(distinct CASE
                    WHEN internet_providers ilike '%Connecticut Education Network%' -- CT state network is provider not applicant
                      then ssd.district_id
                   END) as num_dist_in_netwk,
    count(distinct ssd.district_id) as num_dist_in_state,
    count(distinct CASE
                    WHEN internet_providers ilike '%Connecticut Education Network%'
                      then ssd.district_id
                   END)::numeric/count(distinct ssd.district_id)::numeric as perc_dist_in_netwk,
    array_agg(distinct CASE
                        WHEN internet_providers ilike '%Connecticut Education Network%'
                          then ssd.district_id
                       END) as list_of_dists

    from dm.state_specific_dash_districts ssd

    where funding_year = 2019
    and state_code = 'CT'

    group by 1) aa
on d.state_code = aa.state_code

where d.funding_year = 2019

group by 1,2,3,4

order by aa.state_code asc
