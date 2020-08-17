with unnest_cck12_peers as (
  select distinct on (p.district_id, p.funding_year)
    p.district_id,
    p.funding_year,
    p.peer_id,
    bw.ia_annual_cost_total as incr_cost_peer_ia_annual_cost_total
  from (
    select
      district_id,
      funding_year,
      unnest(bandwidth_suggested_districts) as peer_id
    from ps.districts_peers
  ) p
  join ps.districts_bw_cost bw
  on p.peer_id = bw.district_id
  and p.funding_year = bw.funding_year
  join ps.districts d
  on p.peer_id = d.district_id
  and p.funding_year = d.funding_year
  join ps.districts_fit_for_analysis fit
  on p.peer_id = fit.district_id
  and p.funding_year = fit.funding_year
  where fit.fit_for_ia = true
  and fit.fit_for_ia_cost = true
  and d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
  order by p.district_id, p.funding_year, bw.ia_annual_cost_total desc
),

subset as (
  select
    d.district_id,
    d.num_students,
    dbc.meeting_2014_goal_no_oversub as meeting_2014_goal_no_oversub,
    dbc.ia_monthly_cost_total,
    dbc.ia_annual_cost_erate,
    dbc.ia_annual_cost_total,
    dbc.ia_funding_requested_erate,
    dbc.meeting_knapsack_affordability_target as meeting_knapsack_affordability_target,
    dpr.district_id is not null as have_peer_deal,
    dfa.fit_for_ia_cost as fit_for_ia_cost,
    dfa.fit_for_ia as fit_for_ia,
    cp.incr_cost_peer_ia_annual_cost_total,
    case
      when d.c1_discount_rate is null
        then .7
      else d.c1_discount_rate
    end as c1_discount_rate

  from ps.districts d

  left join ps.districts_bw_cost dbc
  on d.district_id= dbc.district_id
  and d.funding_year = dbc.funding_year

  left join ps.districts_fit_for_analysis dfa
  on d.district_id= dfa.district_id
  and d.funding_year = dfa.funding_year

  left join (
    select distinct
      funding_year,
      district_id
    from ps.districts_peers_ranks
  ) dpr
  on d.district_id= dpr.district_id
  and d.funding_year = dpr.funding_year

  LEFT JOIN unnest_cck12_peers cp
  ON d.district_id = cp.district_id
  AND d.funding_year = cp.funding_year

  where d.funding_year = 2019
  and d.in_universe = true
  and d.district_type = 'Traditional'
),

not_meeting as (
  select
    sample_students_not_meeting,
    sample_students,
     (sum(population_students) OVER ()) - (max(sample_students) OVER ())
     as population_students,
    round(((sum(sample_students_not_meeting) OVER ())/(sum(sample_students) OVER ())*
                    (sum(population_students) OVER ())) -
                      (max(sample_students_not_meeting) OVER ())
           ,0) as population_students_not_meeting
    from (
      select
        sum(num_students::numeric) as population_students,
        sum(num_students::numeric) FILTER (WHERE fit_for_ia = true) as sample_students,
        sum(num_students::numeric) FILTER ( WHERE fit_for_ia = true
                                            AND meeting_2014_goal_no_oversub = false)
          as sample_students_not_meeting
      from subset
    ) sample_pop
),

sample as (
  select
    case
            --either have a peer deal to get them what they need or are spending enough to get bw at benchmark affordability
      when  have_peer_deal = true
        then 'budget sufficient at peer deal'
      else 'increase budget'
    end as subgroup,
    num_students,
    district_id,
    incr_cost_peer_ia_annual_cost_total,
    case
      when have_peer_deal = true
        then 0
      when ia_annual_cost_erate != 0
        then (incr_cost_peer_ia_annual_cost_total*(ia_annual_cost_erate-ia_funding_requested_erate)/ia_annual_cost_erate) -
              case
                when ia_funding_requested_erate > ia_annual_cost_total
                  then 0
                else (ia_annual_cost_total-ia_funding_requested_erate)
              end
      else incr_cost_peer_ia_annual_cost_total*(1-c1_discount_rate)
    end as peer_oop_increase,
    case
      when have_peer_deal = true
        then 0
      else (ia_annual_cost_erate-ia_funding_requested_erate)
    end as oop_increase
  from subset
  where meeting_2014_goal_no_oversub = false
  and fit_for_ia_cost = true
)

select
  count(sample.district_id) as num_districts,
  sum(case
          when subgroup = 'increase budget'
            then 1
          else 0
        end) as no_peer_districts,
  sum(sample.oop_increase)/
    sum(case
          when subgroup = 'increase budget'
            then sample.num_students
          else 0
        end) as oop_per_student_increase,
  sum(sample.peer_oop_increase)/
  sum(case
          when subgroup = 'increase budget'
            then sample.num_students
          else 0
        end) as peer_oop_increase_per_student,
  sum(sample.peer_oop_increase) as sample_peer_oop_increase,
  sum(sample.peer_oop_increase)/sum(sample.num_students)*not_meeting.population_students_not_meeting as extrap_peer_oop_increase,
  sum(case
        when subgroup = 'increase budget'
          then sample.num_students
        else 0
      end) as sample_students_increase_budget,
  sum(sample.num_students) as sample_students,
  not_meeting.population_students_not_meeting as extrap_students

from sample
join not_meeting
on true
group by not_meeting.population_students_not_meeting
