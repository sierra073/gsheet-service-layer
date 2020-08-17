from pandas import read_csv, concat
from numpy import where, logical_and, logical_or, logical_not, isnan, isin

import os
GITHUB = os.environ.get("GITHUB")

import sys
sys.path.insert(0, GITHUB + '/Projects/funding_the_gap/src/features')

from classes import cost_magnifier

unscalable_districts = read_csv(GITHUB + '/''data/unscalable_districts.csv', index_col=0)
district_costs = read_csv(GITHUB + '/''data/district_costs.csv', index_col=0)
print("Distrct costs imported")

district_build_costs = concat([unscalable_districts, district_costs], axis=1)
print("Campuses costs range calculated")

# SOTS 2019: State match/no state match states
full_match_states = ['AZ', 'IL', 'MO', 'WA']
state_match_states = ['AZ', 'CA', 'CO', 'FL', 'ID', 'IL', 'KS', 'MA', 'MD', 'ME',
                      'MO', 'MT', 'NC', 'NH', 'NM', 'NV', 'NY', 'OK', 'OR', 'TX', 'VA', 'WA', 'WI']

# replace null/0 discount rates with 0.7
district_build_costs['c1_discount_rate'] = where(logical_or(district_build_costs['c1_discount_rate'] == 0, isnan(district_build_costs['c1_discount_rate'])),
                                                 0.7,
                                                 district_build_costs['c1_discount_rate'])
district_build_costs['state_match_rate'] = where(district_build_costs['c1_discount_rate'] > .8,
                                                 (1 - district_build_costs['c1_discount_rate']) / 2,
                                                 .1)

# distribute IA costs
district_build_costs['total_cost_ia'] = district_build_costs['district_build_cost'] * district_build_costs['build_fraction_ia'] * cost_magnifier
district_build_costs['total_cost_ia'] = district_build_costs.total_cost_ia.round(decimals=2)
district_build_costs['discount_erate_funding_ia'] = district_build_costs['total_cost_ia'] * district_build_costs['c1_discount_rate']
district_build_costs['discount_erate_funding_ia'] = district_build_costs.discount_erate_funding_ia.round(decimals=2)

district_build_costs['erate_match_ia'] = district_build_costs['total_cost_ia'] * district_build_costs['state_match_rate']

district_build_costs['total_state_funding_ia'] = where(isin(district_build_costs['state_code'], full_match_states),
                                                       district_build_costs['total_cost_ia'] - (district_build_costs['erate_match_ia'] + district_build_costs['discount_erate_funding_ia']),
                                                       where(isin(district_build_costs['state_code'], state_match_states),
                                                             district_build_costs['total_cost_ia'] * district_build_costs['state_match_rate'],
                                                             0))
district_build_costs['total_state_funding_ia'] = district_build_costs.total_state_funding_ia.round(decimals=2)
district_build_costs['total_erate_funding_ia'] = district_build_costs['erate_match_ia'] + district_build_costs['discount_erate_funding_ia']
district_build_costs['total_district_funding_ia'] = district_build_costs['total_cost_ia'] - district_build_costs['total_erate_funding_ia'] - district_build_costs['total_state_funding_ia']
district_build_costs['total_district_funding_ia'] = where((district_build_costs.total_district_funding_ia.round(decimals=2) >= -.01) &
                                                          (district_build_costs.total_district_funding_ia.round(decimals=2) <= .01),
                                                          0,
                                                          district_build_costs['total_district_funding_ia'])

district_build_costs.to_csv(GITHUB + '/''data/id5137_ftg_cost_distributor_ia_2018sots.csv')
