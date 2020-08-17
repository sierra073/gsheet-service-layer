--1 MBPS FUNNEL
--First layer is districts meeting in network states and non network states and not meeting 1 mbps
----Note: those in network states were arbitrarily chosen given a ps.states_static and personal knowledge on procurement style.  This are also excluded from
---- the rest of the funnel since state action can "magically" have all districts meeting 1 mbps and it's not district action
--Second layer is contract expiring
------For those meeting it's whether or not the district had a contract expiring the year prev to them meeting. If they met in 15 than assumption: they did not
------For those not meeting it's whether or not they have a contract expiring at the end of this cycle year or not
--Third layer is peer deals
------For those meeting it's whether or not the district had a peer deal the year prev to them meeting that would get them to 1mbps. If they met in 15 than assumption: no peer deal
------For those not meeting it's whether or not they have a peer deal that would have them meeting 1mbps with oversub
--Forth layer is Service Provider
------For those meeting it's whether or not the district had a peer deal the prev year and if they did if the deal was with the same service provider. If they met in 15 than assumtion: diff sp
------For those not meeting it's whether or not the peer deals they have are with the same provider or a different sp
--Note only the portion not meeting is being extrapolated


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

  FROM
    ps.districts dd

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

  WHERE
    dd.funding_year = 2019
    AND dd.in_universe = true
    AND dd.district_type = 'Traditional'
    AND bw19.meeting_2018_goal_oversub = true
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

  FROM
    ps.districts dd

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

  WHERE
    dd.funding_year = 2019
),

first_table AS (
  SELECT
    dd.district_id,
    CASE
      WHEN dd.state_code in ('AL','CT','DE','GA','HI','KY','ME','NC','ND','NE','RI','SD','SC','UT','WA','WV','WY','TN','MO','MS') THEN 1
    ELSE 0 END AS magic_wand_state,
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
    ELSE 'unknown' END AS having_contract_expiring

  FROM
    ps.districts dd

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

  WHERE
    dd.funding_year = 2019
    AND dd.in_universe = true
    AND dd.district_type = 'Traditional'
),

second_table AS (

  SELECT
    ff.district_id,
    ff.meeting_2018,
    ff.having_contract_expiring,
    COUNT(pr.peer_id) FILTER (WHERE bw.projected_bw_fy2018 <= pr.peer_ia_bw_mbps_total) AS num_peer_deals,
    COUNT(1) FILTER (WHERE  bw.projected_bw_fy2018 <= pr.peer_ia_bw_mbps_total AND pr.peer_service_provider = sp.primary_sp) AS same_sp_deal

  FROM
    first_table ff

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

  WHERE
    ff.meeting_2018 = 'meeting'

  GROUP BY 1,2,3


  UNION


  SELECT
    ff.district_id,
    ff.meeting_2018,
    ff.having_contract_expiring,
    count(1) FILTER (WHERE pr.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS num_peer_deals,
    count(1) FILTER (WHERE pr.current_provider_deal = true AND pr.path_to_meet_2018_goal_group = 'No Cost Peer Deal') AS same_sp_deal

  FROM
    first_table ff

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

  WHERE
    ff.meeting_2018 = 'not_meeting'
    AND ff.fit_for_ia = true
    AND dbw.funding_year = 2019

  GROUP BY 1,2,3
),

--Counts for extrapolation for non state network states
counts_not_meeting AS (
  SELECT
    dd.funding_year,
    COUNT(distinct dd.district_id) AS districts_population,
    COUNT(distinct dd.district_id) FILTER (WHERE ff.fit_for_ia = true) AS districts_sample,
    COUNT(distinct st.district_id) AS districts_sample_not_meeting_2018,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'expiring') AS sam_not_meet_exp_cont,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'not expiring') AS sam_not_meet_not_exp_cont,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'expiring' AND st.num_peer_deals > 0) AS sam_not_meet_exp_cont_peer,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'expiring' AND st.num_peer_deals = 0) AS sam_not_meet_exp_cont_nopeer,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'not expiring' AND st.num_peer_deals > 0) AS sam_not_meet_not_exp_cont_peer,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'not expiring' AND st.num_peer_deals = 0) AS sam_not_meet_not_exp_cont_nopeer,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'expiring' AND st.num_peer_deals > 0 AND same_sp_deal > 0) AS sam_not_meet_exp_cont_peer_same,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'expiring' AND st.num_peer_deals > 0 AND same_sp_deal = 0) AS sam_not_meet_exp_cont_peer_diff,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'not expiring' AND st.num_peer_deals > 0 AND same_sp_deal > 0) AS sam_not_meet_not_exp_cont_peer_same,
    COUNT(distinct st.district_id) FILTER (WHERE st.having_contract_expiring = 'not expiring' AND st.num_peer_deals > 0 AND same_sp_deal = 0) AS sam_not_meet_not_exp_cont_peer_diff

  FROM
    ps.districts dd

    LEFT JOIN second_table st
    ON dd.district_id = st.district_id
    AND st.meeting_2018 = 'not_meeting'

    JOIN ps.districts_fit_for_analysis ff
    ON ff.district_id = dd.district_id
    AND ff.funding_year = dd.funding_year

  WHERE
    dd.in_universe = true
    AND dd.district_type = 'Traditional'
    AND dd.funding_year = 2019
    AND dd.state_code NOT IN ('AL','CT','DE','GA','HI','KY','ME','NC','ND','NE','RI','SD','SC','UT','WA','WV','WY','TN','MO','MS')

  GROUP BY 1
),

----Doing extrapolation in 3 tables to make calculations shorter and easier to read
extrapolate_first_layer AS (
  SELECT
    ROUND(districts_sample_not_meeting_2018::numeric/districts_sample*districts_population) AS districts_not_meeting,
    ROUND((sam_not_meet_exp_cont::numeric/districts_sample_not_meeting_2018::numeric) * (districts_sample_not_meeting_2018::numeric/districts_sample*districts_population)) AS not_meet_exp_cont,
    ROUND(districts_sample_not_meeting_2018::numeric/districts_sample*districts_population)-round((sam_not_meet_exp_cont::numeric/districts_sample_not_meeting_2018::numeric) * (districts_sample_not_meeting_2018::numeric/districts_sample*districts_population)) AS not_meet_not_exp_cont

  FROM
    counts_not_meeting
),

extrapolate_second_layer AS (
  SELECT
    districts_not_meeting,
    not_meet_exp_cont,
    not_meet_not_exp_cont,
    ROUND(sam_not_meet_exp_cont_peer::numeric/sam_not_meet_exp_cont::numeric*not_meet_exp_cont) AS not_meet_exp_cont_peer,
    ROUND(not_meet_exp_cont -(sam_not_meet_exp_cont_peer::numeric/sam_not_meet_exp_cont::numeric*not_meet_exp_cont)) AS not_meet_exp_cont_no_peer,
    ROUND(sam_not_meet_not_exp_cont_peer::numeric/sam_not_meet_not_exp_cont::numeric*not_meet_not_exp_cont) AS not_meet_not_exp_cont_peer,
    ROUND(not_meet_not_exp_cont -(sam_not_meet_not_exp_cont_peer::numeric/sam_not_meet_not_exp_cont::numeric*not_meet_not_exp_cont)) AS not_meet_not_exp_cont_no_peer


  FROM
    counts_not_meeting, extrapolate_first_layer
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
    ROUND(sam_not_meet_exp_cont_peer_same::numeric/sam_not_meet_exp_cont_peer::numeric*not_meet_exp_cont_peer) AS not_meet_exp_cont_peer_same,
    ROUND(not_meet_exp_cont_peer - (sam_not_meet_exp_cont_peer_same::numeric/sam_not_meet_exp_cont_peer::numeric*not_meet_exp_cont_peer)) AS not_meet_exp_cont_peer_diff,
    ROUND(sam_not_meet_not_exp_cont_peer_same::numeric/sam_not_meet_not_exp_cont_peer::numeric*not_meet_not_exp_cont_peer) AS not_meet_not_exp_cont_peer_same,
    ROUND(not_meet_not_exp_cont_peer - (sam_not_meet_not_exp_cont_peer_same::numeric/sam_not_meet_not_exp_cont_peer::numeric*not_meet_not_exp_cont_peer)) AS not_meet_not_exp_cont_peer_diff


  FROM
    counts_not_meeting, extrapolate_second_layer
),


--counts to extrapolate the num districts in "magic_wand_states"

counts_for_state_network AS (

  SELECT
    dd.funding_year,
    COUNT(distinct dd.district_id) AS network_districts_population,
    COUNT(distinct dd.district_id) FILTER (WHERE ff.fit_for_ia = true) AS network_districts_sample,
    COUNT(distinct ft.district_id) AS network_districts_sample_not_meeting_2018

  FROM
    ps.districts dd

    LEFT JOIN first_table ft
    ON dd.district_id = ft.district_id
    AND ft.meeting_2018 = 'not_meeting'

    JOIN ps.districts_fit_for_analysis ff
    ON ff.district_id = dd.district_id
    AND ff.funding_year = dd.funding_year

  WHERE
    dd.in_universe = true
    AND dd.district_type = 'Traditional'
    AND dd.funding_year = 2019
    AND dd.state_code IN ('AL','CT','DE','GA','HI','KY','ME','NC','ND','NE','RI','SD','SC','UT','WA','WV','WY','TN','MO','MS')

  GROUP BY 1
),

meeting AS (

  SELECT
    COUNT(distinct ff.district_id) as total_districts,
    COUNT(distinct ff.district_id) FILTER (WHERE ff.meeting_2018 = 'meeting') as districts_meeting,
    COUNT(distinct ff.district_id) FILTER (WHERE ff.having_contract_expiring = 'expiring' AND ff.meeting_2018 = 'meeting') AS districts_meeting_w_contract_exp,
    COUNT(distinct ff.district_id) FILTER (WHERE ff.having_contract_expiring = 'not expiring' AND ff.meeting_2018 = 'meeting') AS districts_meeting_w_contract_not_exp,
    COUNT(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0) AS districts_meeting_w_contract_exp_w_peer,
    COUNT(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals = 0) AS districts_meeting_w_contract_exp_w_no_peer,
    COUNT(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'not expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0) AS districts_meeting_w_contract_not_exp_w_peer,
    COUNT(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'not expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals = 0) AS districts_meeting_w_contract_not_exp_w_no_peer,
    COUNT(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0 AND ss.same_sp_deal > 0) AS districts_meeting_w_contract_exp_w_peer_same_sp,
    COUNT(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0 AND ss.same_sp_deal = 0) AS districts_meeting_w_contract_exp_w_peer_diff_sp,
    COUNT(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'not expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0 AND ss.same_sp_deal > 0) AS districts_meeting_w_contract_not_exp_w_peer_same_sp,
    COUNT(distinct ss.district_id) FILTER (WHERE ss.having_contract_expiring = 'not expiring' AND ss.meeting_2018 = 'meeting' AND ss.num_peer_deals > 0 AND ss.same_sp_deal = 0) AS districts_meeting_w_contract_not_exp_w_peer_diff_sp

  FROM
    first_table ff

    LEFT JOIN second_table ss
    ON ff.district_id = ss.district_id
)

--joining the not meeting extrapolated table with the meeting table

  SELECT
    total_districts,
--adding the state_network_districts that not meeting to those not meeting in non network states
    ROUND(network_districts_sample_not_meeting_2018::numeric/network_districts_sample*network_districts_population) + districts_not_meeting AS total_districts_not_meeting,
    districts_meeting,
    ROUND(network_districts_sample_not_meeting_2018::numeric/network_districts_sample*network_districts_population) AS network_districts_not_meeting,
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

  FROM
    extrapolate_third_layer,
    counts_for_state_network,
    meeting
