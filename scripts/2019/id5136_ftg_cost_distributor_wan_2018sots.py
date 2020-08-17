from pandas import read_csv, concat, merge
from numpy import where, logical_and, logical_or, logical_not, isnan, isin

import os
GITHUB = os.environ.get("GITHUB")

import sys
sys.path.insert(0, GITHUB + '/Projects/funding_the_gap/src/features')


from classes import cost_magnifier

campus_build_costs = read_csv(GITHUB + '/Projects/sots-isl/data/campus_build_costs_before_distribution.csv', index_col=0)
state_cost_per_mile = read_csv(GITHUB + '/Projects/sots-isl/data/state_cost_per_mile.csv', index_col=0)

# SOTS 2019: State match/no state match states
full_match_states = ['AZ', 'IL', 'MO', 'WA']
state_match_states = ['AZ', 'CA', 'CO', 'FL', 'ID', 'IL', 'KS', 'MA', 'MD', 'ME',
                      'MO', 'MT', 'NC', 'NH', 'NM', 'NV', 'NY', 'OK', 'OR', 'TX', 'VA', 'WA', 'WI']

# fill in cost/mile for campuses without cost calculated
campus_build_costs = merge(campus_build_costs, state_cost_per_mile, how='outer', on='state_code')
campus_build_costs['build_cost_az_pop'] = where(campus_build_costs['build_cost_az_pop'] < 0,
                                                campus_build_costs['build_cost_az_pop_per_mile'] * campus_build_costs['build_distance_az_pop'],
                                                campus_build_costs['build_cost_az_pop'])
campus_build_costs['build_cost_az'] = where(campus_build_costs['build_cost_az'] < 0,
                                            campus_build_costs['build_cost_az_per_mile'] * campus_build_costs['distance'],
                                            campus_build_costs['build_cost_az'])

# calculate state match rate
# replace null/0 discount rates with 0.7
campus_build_costs['c1_discount_rate'] = where(logical_or(campus_build_costs['c1_discount_rate'] == 0, isnan(campus_build_costs['c1_discount_rate'])),
                                               0.7,
                                               campus_build_costs['c1_discount_rate'])
campus_build_costs['state_match_rate'] = where(campus_build_costs['c1_discount_rate'] > .8,
                                               (1 - campus_build_costs['c1_discount_rate']) / 2,
                                               .1)

# distribute A-->PoP-->Z WAN costs
campus_build_costs['total_cost_az_pop_wan'] = where(campus_build_costs['distance'] == 0,
                                                    0,
                                                    campus_build_costs['build_cost_az_pop'] * campus_build_costs['build_fraction_wan'] * cost_magnifier)
campus_build_costs['total_cost_az_pop_wan'] = campus_build_costs['total_cost_az_pop_wan'].round(decimals=2)
campus_build_costs['discount_erate_funding_az_pop_wan'] = campus_build_costs['total_cost_az_pop_wan'] * campus_build_costs['c1_discount_rate']
campus_build_costs['discount_erate_funding_az_pop_wan'] = campus_build_costs['discount_erate_funding_az_pop_wan'].round(decimals=2)
campus_build_costs['erate_match_az_pop_wan'] = campus_build_costs['total_cost_az_pop_wan'] * campus_build_costs['state_match_rate']

campus_build_costs['total_state_funding_az_pop_wan'] = where(isin(campus_build_costs['state_code'], full_match_states),
                                                             campus_build_costs['total_cost_az_pop_wan'] - (campus_build_costs['erate_match_az_pop_wan'] + campus_build_costs['discount_erate_funding_az_pop_wan']),
                                                             where(isin(campus_build_costs['state_code'], state_match_states),
                                                                   campus_build_costs['total_cost_az_pop_wan'] * campus_build_costs['state_match_rate'],
                                                                   0))


campus_build_costs['total_state_funding_az_pop_wan'] = campus_build_costs['total_state_funding_az_pop_wan'].round(decimals=2)
campus_build_costs['total_erate_funding_az_pop_wan'] = campus_build_costs['erate_match_az_pop_wan'] + campus_build_costs['discount_erate_funding_az_pop_wan']
campus_build_costs['total_district_funding_az_pop_wan'] = campus_build_costs['total_cost_az_pop_wan'] - campus_build_costs['total_erate_funding_az_pop_wan'] - campus_build_costs['total_state_funding_az_pop_wan']
campus_build_costs['total_district_funding_az_pop_wan'] = where((campus_build_costs.total_district_funding_az_pop_wan.round(decimals=2) >= -.01) &
                                                                (campus_build_costs.total_district_funding_az_pop_wan.round(decimals=2) <= .01),
                                                                0,
                                                                campus_build_costs['total_district_funding_az_pop_wan'])
# distribute A-->Z WAN costs
campus_build_costs['total_cost_az_wan'] = where(campus_build_costs['distance'] == 0,
                                                0,
                                                campus_build_costs['build_cost_az'] * campus_build_costs['build_fraction_wan'] * cost_magnifier)
campus_build_costs['total_cost_az_wan'] = campus_build_costs['total_cost_az_wan'].round(decimals=2)
campus_build_costs['discount_erate_funding_az_wan'] = campus_build_costs['total_cost_az_wan'] * campus_build_costs['c1_discount_rate']
campus_build_costs['discount_erate_funding_az_wan'] = campus_build_costs['discount_erate_funding_az_wan'].round(decimals=2)

campus_build_costs['erate_match_az_wan'] = campus_build_costs['total_cost_az_wan'] * campus_build_costs['state_match_rate']

campus_build_costs['total_state_funding_az_wan'] = where(isin(campus_build_costs['state_code'], full_match_states),
                                                         campus_build_costs['total_cost_az_wan'] - (campus_build_costs['erate_match_az_wan'] + campus_build_costs['discount_erate_funding_az_wan']),
                                                         where(isin(campus_build_costs['state_code'], state_match_states),
                                                               campus_build_costs['total_cost_az_wan'] * campus_build_costs['state_match_rate'],
                                                               0))

campus_build_costs['total_state_funding_az_wan'] = campus_build_costs['total_state_funding_az_wan'].round(decimals=2)
campus_build_costs['total_erate_funding_az_wan'] = campus_build_costs['erate_match_az_wan'] + campus_build_costs['discount_erate_funding_az_wan']
campus_build_costs['total_district_funding_az_wan'] = campus_build_costs['total_cost_az_wan'] - campus_build_costs['total_erate_funding_az_wan'] - campus_build_costs['total_state_funding_az_wan']
campus_build_costs['total_district_funding_az_wan'] = where((campus_build_costs.total_district_funding_az_wan.round(decimals=2) >= -.01) &
                                                            (campus_build_costs.total_district_funding_az_wan.round(decimals=2) <= .01),
                                                            0,
                                                            campus_build_costs['total_district_funding_az_wan'])

# calculate median cost
campus_build_costs['median_total_cost_wan'] = campus_build_costs[["total_cost_az_pop_wan", "total_cost_az_wan"]].mean(axis=1)
campus_build_costs['total_cost_median_wan'] = campus_build_costs['median_total_cost_wan'].round(decimals=2)
campus_build_costs['median_discount_erate_funding_wan'] = campus_build_costs['median_total_cost_wan'] * campus_build_costs['c1_discount_rate']
campus_build_costs['median_discount_erate_funding_wan'] = campus_build_costs.median_discount_erate_funding_wan.round(decimals=2)

campus_build_costs['erate_match_median_wan'] = campus_build_costs['median_total_cost_wan'] * campus_build_costs['state_match_rate']

campus_build_costs['median_total_state_funding_wan'] = where(isin(campus_build_costs['state_code'], full_match_states),
                                                             campus_build_costs['median_total_cost_wan'] - (campus_build_costs['erate_match_median_wan'] + campus_build_costs['median_discount_erate_funding_wan']),
                                                             where(isin(campus_build_costs['state_code'], state_match_states),
                                                                   campus_build_costs['median_total_cost_wan'] * campus_build_costs['state_match_rate'],
                                                                   0))

campus_build_costs['median_total_state_funding_wan'] = campus_build_costs['median_total_state_funding_wan'].round(decimals=2)
campus_build_costs['median_total_erate_funding_wan'] = campus_build_costs['erate_match_median_wan'] + campus_build_costs['median_discount_erate_funding_wan']
campus_build_costs['median_total_district_funding_wan'] = campus_build_costs['median_total_cost_wan'] - campus_build_costs['median_total_erate_funding_wan'] - campus_build_costs['median_total_state_funding_wan']
campus_build_costs['median_total_district_funding_wan'] = where((campus_build_costs.median_total_district_funding_wan.round(decimals=2) >= -.01) &
                                                                (campus_build_costs.median_total_district_funding_wan.round(decimals=2) <= .01),
                                                                0,
                                                                campus_build_costs['median_total_district_funding_wan'])

campus_build_costs['az_min'] = where(campus_build_costs['total_cost_az_wan'] <= campus_build_costs['total_cost_az_pop_wan'], 1, 0)

# filter for positive costs
campus_build_costs = campus_build_costs[campus_build_costs.total_cost_median_wan >= 0]

campus_build_costs.to_csv(GITHUB + '/Projects/sots-isl/data/id5136_ftg_cost_distributor_wan_2018sots.csv')
