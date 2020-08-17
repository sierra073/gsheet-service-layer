--1 MBPS FUNNEL
--First layer is those districts meeting and not meeting 1 mbps
--Second layer is contract expiring
------For those meeting it's whether or not the district had a contract expiring the year prev to them meeting. If they met in 15 than assumption: they did not
------For those not meeting it's whether or not they have a contract expiring at the end of this cycle year or not
--Third layer is peer deals
------For those meeting it's whether or not the district had a peer deal the year prev to them meeting that would get them to 1mbps. If they met in 15 than assumption: no peer deal
------For those not meeting it's whether or not they have a peer deal that would have them meeting 1mbps with oversub
--Forth layer is Service Provider
------For those meeting it's whether or not the district had a peer deal the prev year and if they did if the deal was with the same service provider. If they met in 15 than assumtion: diff sp
------For those not meeting it's whether or not the peer deals they have are with the same provider or a different sp



WITH year_meeting AS (
SELECT
  dd.district_id,
  CASE
    WHEN bw15.meeting_2018_goal_oversub = true AND ff15.fit_for_ia = true THEN 2015
    WHEN bw16.meeting_2018_goal_oversub = true AND ff16.fit_for_ia = true THEN 2016
    WHEN bw17.meeting_2018_goal_oversub = true AND ff17.fit_for_ia = true THEN 2017
    WHEN bw18.meeting_2018_goal_oversub = true AND ff18.fit_for_ia = true THEN 2018
    WHEN bw19.meeting_2018_goal_oversub = true AND ff19.fit_for_ia = true THEN 2019
    END AS year_meeting_1mbps

FROM ps.districts dd

LEFT JOIN ps.districts_bw_cost bw15
ON dd.district_id = bw15.district_id
AND bw15.funding_year = 2015

LEFT JOIN ps.districts_fit_for_analysis ff15
ON bw15.district_id = ff15.district_id
AND bw15.funding_year = ff15.funding_year

LEFT JOIN ps.districts_bw_cost bw16
ON dd.district_id = bw16.district_id
AND bw16.funding_year = 2016

LEFT JOIN ps.districts_fit_for_analysis ff16
ON bw16.district_id = ff16.district_id
AND bw16.funding_year = ff16.funding_year

LEFT JOIN ps.districts_bw_cost bw17
ON dd.district_id = bw17.district_id
AND bw17.funding_year = 2017

LEFT JOIN ps.districts_fit_for_analysis ff17
ON bw17.district_id = ff17.district_id
AND bw17.funding_year = ff17.funding_year

LEFT JOIN ps.districts_bw_cost bw18
ON dd.district_id = bw18.district_id
AND bw18.funding_year = 2018

LEFT JOIN ps.districts_fit_for_analysis ff18
ON bw18.district_id = ff18.district_id
AND bw18.funding_year = ff18.funding_year

JOIN ps.districts_bw_cost bw19
ON dd.district_id = bw19.district_id
AND bw19.funding_year = dd.funding_year

JOIN ps.districts_fit_for_analysis ff19
ON bw19.district_id = ff19.district_id
AND bw19.funding_year = ff19.funding_year

WHERE dd.funding_year = 2019
and dd.in_universe = true
and dd.district_type = 'Traditional'
and bw19.meeting_2018_goal_oversub = true
),

expiration AS (
SELECT
  dd.district_id,
  yy.year_meeting_1mbps,
  CASE
    WHEN yy.year_meeting_1mbps = 2016 AND dl15.most_recent_primary_ia_contract_end_date >= '2016-06-30' THEN 'expiring'
    WHEN yy.year_meeting_1mbps = 2017 AND dl16.most_recent_primary_ia_contract_end_date >= '2017-06-30' THEN 'expiring'
    WHEN yy.year_meeting_1mbps = 2018 AND dl17.most_recent_primary_ia_contract_end_date >= '2018-06-30' THEN 'expiring'
    WHEN yy.year_meeting_1mbps = 2019 AND dl18.most_recent_primary_ia_contract_end_date >= '2019-06-30' THEN 'expiring'
    ELSE 'not expiring' END AS contract_expiring

FROM ps.districts dd

JOIN year_meeting yy
ON dd.district_id = yy.district_id

LEFT JOIN ps.districts_lines dl15
ON dd.district_id = dl15.district_id
AND dl15.funding_year = 2015

LEFT JOIN ps.districts_lines dl16
ON dd.district_id = dl16.district_id
AND dl16.funding_year = 2016

LEFT JOIN ps.districts_lines dl17
ON dd.district_id = dl17.district_id
AND dl17.funding_year = 2017

LEFT JOIN ps.districts_lines dl18
ON dd.district_id = dl18.district_id
AND dl18.funding_year = 2018

LEFT JOIN ps.districts_lines dl19
ON dd.district_id = dl19.district_id
AND dl19.funding_year = 2019

WHERE dd.funding_year = 2019
),

first_table AS (
SELECT
  dd.district_id,
  CASE
    WHEN bb.meeting_2018_goal_oversub = false AND ff.fit_for_ia = true THEN 'not_meeting'
    WHEN ee.year_meeting_1mbps IS NOT NULL THEN 'meeting'
    ELSE 'unknown' END as meeting_2018,
  ff.fit_for_ia,
  ee.year_meeting_1mbps,
  CASE
    WHEN ee.contract_expiring IS NOT NULL THEN ee.contract_expiring
    WHEN li.most_recent_primary_ia_contract_end_date <= '2020-06-30' THEN 'expiring'
    WHEN (li.most_recent_primary_ia_contract_end_date > '2020-06-30' OR li.most_recent_primary_ia_contract_end_date ISNULL) THEN 'not expiring'
    ELSE 'unknown' END as having_contract_expiring


FROM ps.districts dd

JOIN ps.districts_bw_cost bb
ON dd.district_id = bb.district_id
AND dd.funding_year = bb.funding_year

JOIN ps.districts_fit_for_analysis ff
ON ff.district_id = dd.district_id
AND ff.funding_year = dd.funding_year

JOIN ps.districts_lines li
ON li.district_id = dd.district_id
AND li.funding_year = dd.funding_year

LEFT JOIN expiration ee
ON ee.district_id = dd.district_id

WHERE dd.funding_year = 2019
AND dd.in_universe = true
AND dd.district_type = 'Traditional'
),


second_table AS (

SELECT
  ff.district_id,
  ff.meeting_2018,
  ff.having_contract_expiring,
  count(pr.peer_id) FILTER (WHERE bw.projected_bw_fy2018 <= pr.peer_ia_bw_mbps_total) AS num_peer_deals,
  count(1) FILTER (WHERE  bw.projected_bw_fy2018 <= pr.peer_ia_bw_mbps_total AND pr.peer_service_provider = sp.primary_sp) AS same_sp_deal

FROM first_table ff

LEFT JOIN ps.districts_sp_assignments sp
ON ff.district_id = sp.district_id
AND ff.year_meeting_1mbps = sp.funding_year + 1

LEFT JOIN ps.districts_peers_ranks pr
ON ff.district_id = pr.district_id
AND ff.year_meeting_1mbps = pr.funding_year + 1

LEFT JOIN ps.districts_bw_cost bw
ON ff.district_id = bw.district_id
AND ff.year_meeting_1mbps = bw.funding_year + 1

LEFT JOIN ps.districts dd
ON ff.district_id = dd.district_id
AND ff.year_meeting_1mbps = dd.funding_year + 1

WHERE ff.meeting_2018 = 'meeting'

GROUP BY 1,2,3


UNION


SELECT
  ff.district_id,
  ff.meeting_2018,
  ff.having_contract_expiring,
  count(1) FILTER (WHERE pr.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS num_peer_deals,
  count(1) FILTER (WHERE pr.current_provider_deal = true AND pr.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS same_sp_deal

FROM first_table ff

JOIN ps.districts_bw_cost dbw
ON ff.district_id = dbw.district_id

JOIN ps.districts dd
ON dd.district_id = dbw.district_id
AND dd.funding_year = dbw.funding_year

JOIN ps.districts_sp_assignments sp
ON dbw.district_id = sp.district_id
AND dbw.funding_year = sp.funding_year

LEFT JOIN ps.districts_upgrades pr
ON dbw.district_id = pr.district_id
AND dbw.funding_year = pr.funding_year

WHERE ff.meeting_2018 = 'not_meeting'
AND ff.fit_for_ia = true
AND dbw.funding_year = 2019
GROUP BY 1,2,3
),


counts_not_meeting AS (
SELECT
  dd.funding_year,
  count(distinct dd.district_id) AS districts_population,
  count(distinct dd.district_id) FILTER (WHERE ff.fit_for_ia = true) as districts_sample,
  count(distinct st.district_id) AS districts_sample_not_meeting_2018,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'expiring') AS sam_not_meet_exp_cont,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'not expiring') AS sam_not_meet_not_exp_cont,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'expiring' AND st.num_peer_deals > 0) AS sam_not_meet_exp_cont_peer,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'expiring' AND st.num_peer_deals = 0) AS sam_not_meet_exp_cont_nopeer,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'not expiring' AND st.num_peer_deals > 0) AS sam_not_meet_not_exp_cont_peer,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'not expiring' AND st.num_peer_deals = 0) AS sam_not_meet_not_exp_cont_nopeer,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'expiring' AND st.num_peer_deals > 0 AND same_sp_deal > 0) AS sam_not_meet_exp_cont_peer_same,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'expiring' AND st.num_peer_deals > 0 AND same_sp_deal = 0) AS sam_not_meet_exp_cont_peer_diff,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'not expiring' AND st.num_peer_deals > 0 AND same_sp_deal > 0) AS sam_not_meet_not_exp_cont_peer_same,
  count(distinct st.district_id) FILTER (where st.having_contract_expiring = 'not expiring' AND st.num_peer_deals > 0 AND same_sp_deal = 0) AS sam_not_meet_not_exp_cont_peer_diff

FROM ps.districts dd

LEFT JOIN second_table st
ON dd.district_id = st.district_id
AND st.meeting_2018 = 'not_meeting'

JOIN ps.districts_fit_for_analysis ff
ON ff.district_id = dd.district_id
AND ff.funding_year = dd.funding_year

WHERE dd.in_universe = true
AND dd.district_type = 'Traditional'
AND dd.funding_year = 2019
GROUP BY 1
),


extrapolate_first_layer AS (
SELECT
  round(districts_sample_not_meeting_2018::numeric/districts_sample*districts_population) AS districts_not_meeting,
  round((sam_not_meet_exp_cont::numeric/districts_sample_not_meeting_2018::numeric) * (districts_sample_not_meeting_2018::numeric/districts_sample*districts_population)) AS not_meet_exp_cont,
  round(districts_sample_not_meeting_2018::numeric/districts_sample*districts_population)-round((sam_not_meet_exp_cont::numeric/districts_sample_not_meeting_2018::numeric) * (districts_sample_not_meeting_2018::numeric/districts_sample*districts_population)) AS not_meet_not_exp_cont

FROM counts_not_meeting
),

extrapolate_second_layer AS (
SELECT
  districts_not_meeting,
  not_meet_exp_cont,
  not_meet_not_exp_cont,
  round(sam_not_meet_exp_cont_peer::numeric/sam_not_meet_exp_cont::numeric*not_meet_exp_cont) AS not_meet_exp_cont_peer,
  round(not_meet_exp_cont -(sam_not_meet_exp_cont_peer::numeric/sam_not_meet_exp_cont::numeric*not_meet_exp_cont)) AS not_meet_exp_cont_no_peer,
  round(sam_not_meet_not_exp_cont_peer::numeric/sam_not_meet_not_exp_cont::numeric*not_meet_not_exp_cont) AS not_meet_not_exp_cont_peer,
  round(not_meet_not_exp_cont -(sam_not_meet_not_exp_cont_peer::numeric/sam_not_meet_not_exp_cont::numeric*not_meet_not_exp_cont)) AS not_meet_not_exp_cont_no_peer


FROM counts_not_meeting, extrapolate_first_layer
),


extrapolate_third_layer AS (

SELECT
  districts_not_meeting,
  not_meet_exp_cont,
  not_meet_not_exp_cont,
  not_meet_exp_cont_peer,
  not_meet_exp_cont_no_peer,
  not_meet_not_exp_cont_peer,
  not_meet_not_exp_cont_no_peer,
  round(sam_not_meet_exp_cont_peer_same::numeric/sam_not_meet_exp_cont_peer::numeric*not_meet_exp_cont_peer) AS not_meet_exp_cont_peer_same,
  round(not_meet_exp_cont_peer - (sam_not_meet_exp_cont_peer_same::numeric/sam_not_meet_exp_cont_peer::numeric*not_meet_exp_cont_peer)) AS not_meet_exp_cont_peer_diff,
  round(sam_not_meet_not_exp_cont_peer_same::numeric/sam_not_meet_not_exp_cont_peer::numeric*not_meet_not_exp_cont_peer) AS not_meet_not_exp_cont_peer_same,
  round(not_meet_not_exp_cont_peer - (sam_not_meet_not_exp_cont_peer_same::numeric/sam_not_meet_not_exp_cont_peer::numeric*not_meet_not_exp_cont_peer)) AS not_meet_not_exp_cont_peer_diff


FROM counts_not_meeting, extrapolate_second_layer
),


meeting AS (

SELECT
--total number of districts
  count(distinct ff.district_id) AS total_districts,
--total meeting
  count(distinct ff.district_id) FILTER (WHERE ff.meeting_2018 = 'meeting') AS districts_meeting,
--total meeting and contract exp
  count(distinct ff.district_id) FILTER (WHERE ff.having_contract_expiring = 'expiring' AND ff.meeting_2018 = 'meeting') AS districts_meeting_w_contract_exp,
--total meeting and contract not exp
  count(distinct ff.district_id) FILTER (WHERE ff.having_contract_expiring = 'not expiring' AND ff.meeting_2018 = 'meeting') AS districts_meeting_w_contract_not_exp,
--total meeting and contract exp w peer deal
  count(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0) AS districts_meeting_w_contract_exp_w_peer,
--total meeting and contract exp w no peer deal
  count(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals = 0) AS districts_meeting_w_contract_exp_w_no_peer,
--total meeting and contract not exp w peer deal
  count(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'not expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0) AS districts_meeting_w_contract_not_exp_w_peer,
--total meeting and contract not exp w no peer deal
  count(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'not expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals = 0) AS districts_meeting_w_contract_not_exp_w_no_peer,
--total meeting and contract exp w peer deal w same sp
  count(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0 AND ss.same_sp_deal > 0) AS districts_meeting_w_contract_exp_w_peer_same_sp,
--total meeting and contract exp w peer deal w diff sp
  count(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0 AND ss.same_sp_deal = 0) AS districts_meeting_w_contract_exp_w_peer_diff_sp,
--total meeting and contract not exp w peer deal w same sp
  count(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'not expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0 AND ss.same_sp_deal > 0) AS districts_meeting_w_contract_not_exp_w_peer_same_sp,
--total meeting and contract not exp w peer deal w diff sp
  count(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'not expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0 AND ss.same_sp_deal = 0) AS districts_meeting_w_contract_not_exp_w_peer_diff_sp

FROM first_table ff

LEFT JOIN second_table ss
ON ff.district_id = ss.district_id
)

SELECT
  total_districts,
  districts_not_meeting,
  districts_meeting,
  not_meet_exp_cont AS districts_not_meeting_w_contract_exp,
  not_meet_not_exp_cont AS districts_not_meeting_w_contract_not_exp,
  districts_meeting_w_contract_exp,
  districts_meeting_w_contract_not_exp,
  not_meet_exp_cont_peer AS districts_not_meeting_w_contract_exp_w_peer,
  not_meet_exp_cont_no_peer AS districts_not_meeting_w_contract_exp_w_no_peer,
  not_meet_not_exp_cont_peer AS districts_not_meeting_w_contract_not_exp_w_peer,
  not_meet_not_exp_cont_no_peer AS districts_not_meeting_w_contract_not_exp_w_no_peer,
  districts_meeting_w_contract_exp_w_peer,
  districts_meeting_w_contract_exp_w_no_peer,
  districts_meeting_w_contract_not_exp_w_peer,
  districts_meeting_w_contract_not_exp_w_no_peer,
  not_meet_exp_cont_peer_same AS districts_not_meeting_w_contract_exp_w_peer_same_sp,
  not_meet_exp_cont_peer_diff AS districts_not_meeting_w_contract_exp_w_peer_diff_sp,
  not_meet_not_exp_cont_peer_same AS districts_not_meeting_w_contract_not_exp_w_peer_same_sp,
  not_meet_not_exp_cont_peer_diff AS districts_not_meeting_w_contract_not_exp_w_peer_diff_sp,
  districts_meeting_w_contract_exp_w_peer_same_sp,
  districts_meeting_w_contract_exp_w_peer_diff_sp,
  districts_meeting_w_contract_not_exp_w_peer_same_sp,
  districts_meeting_w_contract_not_exp_w_peer_diff_sp

FROM extrapolate_third_layer, meeting
