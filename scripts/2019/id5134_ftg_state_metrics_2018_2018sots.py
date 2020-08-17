from pandas import DataFrame, merge, read_csv
from numpy import NaN, where, logical_or

import os
GITHUB = os.environ.get("GITHUB")

districts = read_csv(GITHUB+'/Projects/sots-isl/data/districts.csv',index_col=0)
print("Districts imported")

##EXTRAPOLATION

#determine number of campuses in clean districts for extrapolation
state_clean = districts[districts['denomination'].isin(['1: Fit for FTG, Target', '2: Fit for FTG, Not Target'])]
state_clean = state_clean[state_clean['fit_for_ia'] == True]
state_clean = state_clean.groupby(['state_code']).sum()
state_clean['clean_num_campuses'] = state_clean['num_campuses']
state_clean = state_clean[['clean_num_campuses']]
state_clean = state_clean.reset_index()

#join number of campuses
state_metrics = state_clean

##ORIG EXTRAPOLATION

#determine number of campuses in clean districts for original extrapolation
state_clean_orig = districts[districts['fit_for_ia'] == True]
state_clean_orig = state_clean_orig.groupby(['state_code']).sum()
state_clean_orig['clean_orig_num_campuses'] = state_clean_orig['num_campuses']
state_clean_orig = state_clean_orig[['clean_orig_num_campuses']]
state_clean_orig = state_clean_orig.reset_index()

#determine number of campuses that need original extrapolation
state_extrapolate_orig = districts[districts['fit_for_ia'] == False]
state_extrapolate_orig = state_extrapolate_orig.groupby(['state_code']).sum()
state_extrapolate_orig['extrapolate_orig_num_campuses'] = state_extrapolate_orig['num_campuses']
state_extrapolate_orig = state_extrapolate_orig[['extrapolate_orig_num_campuses']]
state_extrapolate_orig = state_extrapolate_orig.reset_index()

#join number of campuses or original extrapolation
state_metrics_orig = merge(state_extrapolate_orig, state_clean_orig, how='outer', on='state_code')
state_metrics_orig['extrapolation_orig'] = (state_metrics_orig['clean_orig_num_campuses'] + state_metrics_orig['extrapolate_orig_num_campuses'])/state_metrics_orig['clean_orig_num_campuses']
state_metrics_orig['extrapolation_orig'] = state_metrics_orig.extrapolation_orig.replace(NaN, 1)

#combine extrapolations
state_metrics = merge(state_metrics_orig, state_metrics, how='outer', on='state_code')
print("Extrapolations calculated")

state_metrics.to_csv(GITHUB+'/Projects/sots-isl/data/state_extrapolations.csv')

##WAN COST

#import campus build costs
campus_build_costs = read_csv(GITHUB+'/Projects/sots-isl/data/id5136_ftg_cost_distributor_wan.csv',index_col=0)
print("Campus costs imported")

#determine min and max values
campus_build_costs['min_total_cost_wan'] = campus_build_costs[['total_cost_az_pop_wan', 'total_cost_az_wan']].min(axis=1)
campus_build_costs['min_discount_erate_funding_wan'] = where( campus_build_costs['total_cost_az_wan']>campus_build_costs['total_cost_az_pop_wan'],
                                campus_build_costs['discount_erate_funding_az_pop_wan'],
                                campus_build_costs['discount_erate_funding_az_wan'])
campus_build_costs['min_total_state_funding_wan'] = where(  campus_build_costs['total_cost_az_wan']>campus_build_costs['total_cost_az_pop_wan'],
                              campus_build_costs['total_state_funding_az_pop_wan'],
                              campus_build_costs['total_state_funding_az_wan'])
campus_build_costs['min_total_erate_funding_wan'] = where(  campus_build_costs['total_cost_az_wan']>campus_build_costs['total_cost_az_pop_wan'],
                              campus_build_costs['total_erate_funding_az_pop_wan'],
                              campus_build_costs['total_erate_funding_az_wan'])
campus_build_costs['min_total_district_funding_wan'] = where( campus_build_costs['total_cost_az_wan']>campus_build_costs['total_cost_az_pop_wan'],
                                campus_build_costs['total_district_funding_az_pop_wan'],
                                campus_build_costs['total_district_funding_az_wan'])
campus_build_costs['min_builds_wan'] = where( campus_build_costs['total_cost_az_wan']>campus_build_costs['total_cost_az_pop_wan'],
                            2,
                            1) * campus_build_costs['build_fraction_wan']
campus_build_costs['min_build_distance_wan'] = where( campus_build_costs['total_cost_az_wan']>campus_build_costs['total_cost_az_pop_wan'],
                            campus_build_costs['build_distance_az_pop'] * campus_build_costs['build_fraction_wan'],
                            campus_build_costs['distance'] * campus_build_costs['build_fraction_wan'])
campus_build_costs['max_total_cost_wan'] = campus_build_costs[['total_cost_az_pop_wan', 'total_cost_az_wan']].max(axis=1)
campus_build_costs['max_discount_erate_funding_wan'] = where( campus_build_costs['total_cost_az_wan']<=campus_build_costs['total_cost_az_pop_wan'],
                                campus_build_costs['discount_erate_funding_az_pop_wan'],
                                campus_build_costs['discount_erate_funding_az_wan'])
campus_build_costs['max_total_state_funding_wan'] = where(  campus_build_costs['total_cost_az_wan']<=campus_build_costs['total_cost_az_pop_wan'],
                              campus_build_costs['total_state_funding_az_pop_wan'],
                              campus_build_costs['total_state_funding_az_wan'])
campus_build_costs['max_total_erate_funding_wan'] = where(  campus_build_costs['total_cost_az_wan']<=campus_build_costs['total_cost_az_pop_wan'],
                              campus_build_costs['total_erate_funding_az_pop_wan'],
                              campus_build_costs['total_erate_funding_az_wan'])
campus_build_costs['max_total_district_funding_wan'] = where( campus_build_costs['total_cost_az_wan']<=campus_build_costs['total_cost_az_pop_wan'],
                                campus_build_costs['total_district_funding_az_pop_wan'],
                                campus_build_costs['total_district_funding_az_wan'])
campus_build_costs['max_builds_wan'] = where( campus_build_costs['total_cost_az_wan']<=campus_build_costs['total_cost_az_pop_wan'],
                            2,
                            1) * campus_build_costs['build_fraction_wan']
campus_build_costs['max_build_distance_wan'] = where( campus_build_costs['total_cost_az_wan']<=campus_build_costs['total_cost_az_pop_wan'],
                            campus_build_costs['build_distance_az_pop'] * campus_build_costs['build_fraction_wan'],
                            campus_build_costs['distance'] * campus_build_costs['build_fraction_wan'])
campus_build_costs['builds_wan'] = campus_build_costs['build_fraction_wan']
campus_build_costs['builds_az_wan'] = campus_build_costs['build_fraction_wan']

#create factors for a-->z and a-->pop-->z aggregation
campus_build_costs['builds_az_pop_wan'] = 2 * campus_build_costs['build_fraction_wan']
campus_build_costs['build_distance_az_wan'] = campus_build_costs['distance'] * campus_build_costs['build_fraction_wan']
campus_build_costs['build_distance_az_pop_wan'] = campus_build_costs['build_distance_az_pop'] * campus_build_costs['build_fraction_wan']
campus_build_costs.to_csv(GITHUB+'/Projects/sots-isl/data/campus_build_costs_max_min.csv')
print("Min max calculated")

##
#determine state cost amounts
state_wan_costs = campus_build_costs.groupby(['state_code']).sum()
state_wan_costs = state_wan_costs[['fit_for_ia','min_total_cost_wan', 'min_discount_erate_funding_wan', 'min_total_state_funding_wan', 'min_total_erate_funding_wan',
'min_total_district_funding_wan', 'median_total_erate_funding_wan', 'median_discount_erate_funding_wan','median_total_cost_wan','median_total_state_funding_wan', 'median_total_district_funding_wan', 'min_builds_wan', 'min_build_distance_wan', 'builds_wan', 'builds_az_wan',
'max_total_cost_wan', 'max_discount_erate_funding_wan', 'max_total_state_funding_wan', 'max_total_erate_funding_wan',
'max_total_district_funding_wan', 'max_builds_wan', 'max_build_distance_wan',
'total_cost_az_wan', 'discount_erate_funding_az_wan', 'total_state_funding_az_wan', 'total_erate_funding_az_wan',
'total_district_funding_az_wan',  'build_fraction_wan', 'build_distance_az_wan',
'total_cost_az_pop_wan', 'discount_erate_funding_az_pop_wan', 'total_state_funding_az_pop_wan', 'total_erate_funding_az_pop_wan',
'total_district_funding_az_pop_wan',  'builds_az_pop_wan', 'build_distance_az_pop_wan', 'campus_student_count']]
state_wan_costs = state_wan_costs.reset_index()
state_metrics = merge(state_metrics, state_wan_costs, how='outer', on='state_code')
state_metrics = state_metrics.reset_index(drop=True)

##IA COST

#import district build costs
district_build_costs = read_csv(GITHUB+'/Projects/sots-isl/data/id5137_ftg_cost_distributor_ia.csv',index_col=0)
print("District costs imported")

#determine cost amounts
state_ia_costs = district_build_costs.groupby(['state_code', 'fit_for_ia']).sum()
state_ia_costs = state_ia_costs[['total_cost_ia', 'discount_erate_funding_ia', 'total_state_funding_ia', 'total_erate_funding_ia', 'total_district_funding_ia','build_fraction_ia', 'district_build_distance', 'num_students']]
state_ia_costs = state_ia_costs.reset_index()

#determine clean cost amounts for extrapolation  and extrapolate
state_clean_ia_costs = state_ia_costs[state_ia_costs['fit_for_ia'] == True]
state_metrics = merge(state_metrics, state_clean_ia_costs, how='outer', on=['state_code'])
state_metrics.reset_index(drop=True)

#extrapolate for clean only for orig, and add back in dirtys for not_orig. min and max needs to be done after
state_metrics['extrapolated_total_cost_ia'] = state_metrics['total_cost_ia'] * state_metrics['extrapolation_orig']
state_metrics['extrapolated_discount_erate_funding_ia'] = state_metrics['discount_erate_funding_ia'] * state_metrics['extrapolation_orig']
state_metrics['extrapolated_total_state_funding_ia'] = state_metrics['total_state_funding_ia'] * state_metrics['extrapolation_orig']
state_metrics['extrapolated_total_erate_funding_ia'] = state_metrics['total_erate_funding_ia'] * state_metrics['extrapolation_orig']
state_metrics['extrapolated_total_district_funding_ia'] = state_metrics['total_district_funding_ia'] * state_metrics['extrapolation_orig']
state_metrics['extrapolated_builds_ia'] = state_metrics['build_fraction_ia'].replace(NaN, 0) * state_metrics['extrapolation_orig']
state_metrics['extrapolated_build_distance_ia'] = state_metrics['district_build_distance'].replace(NaN, 0) * state_metrics['extrapolation_orig']


#determine dirty cost amounts for and add to extrapolation
state_dirty_ia_costs = state_ia_costs[state_ia_costs['fit_for_ia'] == False]

state_metrics = merge(state_metrics, state_dirty_ia_costs, how='outer', on=['state_code'])
state_metrics=state_metrics.reset_index(drop=True)
print("District costs extrapolated")

#define number of students impacted - if a district has IA build, then use district num_students, else use campus student counts
campus_build_costs_district = campus_build_costs.groupby(['district_id','state_code']).sum()
campus_build_costs_district = campus_build_costs_district.reset_index()
num_students_by_campus = merge(campus_build_costs_district, district_build_costs[['district_id','build_fraction_ia']], left_on = 'district_id', right_on = 'district_id', how='left')
num_students_by_campus.reset_index(drop=True)
num_students_by_campus['num_students_impacted'] = where(num_students_by_campus['build_fraction_ia'] > 0, num_students_by_campus['num_students'], num_students_by_campus['campus_student_count'])
num_students_by_campus = num_students_by_campus.groupby(['state_code']).sum()
num_students_by_campus = num_students_by_campus.reset_index()

state_metrics = merge(state_metrics, num_students_by_campus[['state_code', 'num_students_impacted']] , how='outer', on=['state_code'])
state_metrics = state_metrics.reset_index(drop=True)

#add IA and WAN costs
#SOTS 2019: DONT DO ANY MULTIPLIERS
def total_cost_metric(col,min_max):
  out_col = 'extrapolated_' + min_max + '_' + col
  ia_col = 'extrapolated_' + col + '_ia'
  wan_col = min_max + '_' + col + '_wan' 
  state_metrics[out_col] = state_metrics[ia_col].replace(NaN, 0) + state_metrics[wan_col]

for col in ['total_cost','discount_erate_funding','total_erate_funding','total_state_funding','total_district_funding']:  
  for min_max in ['min','max', 'median']:
    total_cost_metric(col,min_max)

state_metrics['extrapolated_min_builds'] =  state_metrics['extrapolated_builds_ia'].replace(NaN, 0) + state_metrics['min_builds_wan']
state_metrics['extrapolated_min_builds_1'] =  state_metrics['extrapolated_builds_ia'].replace(NaN, 0) + state_metrics['builds_wan']
state_metrics['extrapolated_min_build_distance'] =  state_metrics['extrapolated_build_distance_ia'].replace(NaN, 0) + state_metrics['min_build_distance_wan']
state_metrics['min_total_cost_per_mile'] =  state_metrics['extrapolated_min_total_cost'] / state_metrics['extrapolated_min_build_distance']
state_metrics['min_miles_per_build'] =  state_metrics['extrapolated_min_build_distance'] / state_metrics['extrapolated_min_builds']
state_metrics['min_miles_per_build_1'] =  state_metrics['extrapolated_min_build_distance'] / state_metrics['extrapolated_min_builds_1']

state_metrics['extrapolated_max_builds'] =  state_metrics['extrapolated_builds_ia'].replace(NaN, 0) + state_metrics['max_builds_wan']
state_metrics['extrapolated_max_builds_1'] =  state_metrics['extrapolated_builds_ia'].replace(NaN, 0) + state_metrics['builds_wan']
state_metrics['extrapolated_max_build_distance'] =  state_metrics['extrapolated_build_distance_ia'].replace(NaN, 0) + state_metrics['max_build_distance_wan']
state_metrics['max_total_cost_per_mile'] =  state_metrics['extrapolated_max_total_cost'] / state_metrics['extrapolated_max_build_distance']
state_metrics['max_miles_per_build'] =  state_metrics['extrapolated_max_build_distance'] / state_metrics['extrapolated_max_builds']
state_metrics['max_miles_per_build_1'] =  state_metrics['extrapolated_max_build_distance'] / state_metrics['extrapolated_max_builds_1']

state_metrics = state_metrics[['state_code', 'extrapolated_min_total_cost', 'extrapolated_min_total_state_funding', 'extrapolated_min_total_erate_funding',
                'extrapolated_min_total_district_funding', 'builds_wan', 'extrapolated_min_builds','extrapolated_min_builds_1', 'extrapolated_min_build_distance', 'min_total_cost_per_mile', 'min_miles_per_build','min_miles_per_build_1',
                'extrapolated_max_total_cost', 'extrapolated_max_total_state_funding', 'extrapolated_max_total_erate_funding',
                'extrapolated_max_total_district_funding', 
                'extrapolated_median_total_cost', 'extrapolated_median_total_state_funding', 'extrapolated_median_total_erate_funding', 'extrapolated_median_total_district_funding',
                'extrapolated_max_builds', 'extrapolated_max_builds_1','extrapolated_max_build_distance', 'max_total_cost_per_mile', 'max_miles_per_build','max_miles_per_build_1',
                'max_total_cost_wan', 'max_total_state_funding_wan', 'max_total_erate_funding_wan',
                'max_total_district_funding_wan', 'max_builds_wan', 'max_build_distance_wan',
                'min_total_cost_wan', 'min_total_state_funding_wan', 'min_total_erate_funding_wan',
                'min_total_district_funding_wan', 'min_builds_wan', 'min_build_distance_wan',
                'extrapolated_total_cost_ia', 'extrapolated_total_state_funding_ia', 'extrapolated_total_erate_funding_ia',
                'extrapolated_total_district_funding_ia', 'extrapolated_build_distance_ia',
                'total_cost_az_wan', 'total_state_funding_az_wan', 'total_erate_funding_az_wan',
                'total_district_funding_az_wan', 'builds_az_wan', 'build_distance_az_wan',
                'total_cost_az_pop_wan', 'total_state_funding_az_pop_wan', 'total_erate_funding_az_pop_wan',
                'total_district_funding_az_pop_wan', 'builds_az_pop_wan', 'build_distance_az_pop_wan',
                'extrapolated_builds_ia', 'num_students_impacted'
                ]]

state_metrics.columns = [ 'state_code', 'min_total_cost', 'min_total_state_funding', 'min_total_erate_funding',
              'min_total_district_funding', 'builds_wan', 'min_builds','min_builds_1', 'min_build_distance', 'min_total_cost_per_mile', 'min_miles_per_build', 'min_miles_per_build_1',
              'max_total_cost', 'max_total_state_funding', 'max_total_erate_funding',
              'max_total_district_funding',
                            'total_cost_median', 'total_state_funding_median', 'total_erate_funding_median', 'total_district_funding_median',
              'max_builds','max_builds_1', 'max_build_distance', 'max_total_cost_per_mile', 'max_miles_per_build', 'max_miles_per_build_1',
              'max_total_cost_wan', 'max_total_state_funding_wan', 'max_total_erate_funding_wan',
              'max_total_district_funding_wan', 'max_builds_wan', 'max_build_distance_wan',
              'min_total_cost_wan', 'min_total_state_funding_wan', 'min_total_erate_funding_wan',
              'min_total_district_funding_wan', 'min_builds_wan', 'min_build_distance_wan',
              'total_cost_ia', 'total_state_funding_ia', 'total_erate_funding_ia',
              'total_district_funding_ia', 'build_distance_ia',
              'total_cost_az_wan', 'total_state_funding_az_wan', 'total_erate_funding_az_wan',
              'total_district_funding_az_wan', 'builds_az_wan', 'build_distance_az_wan',
              'total_cost_az_pop_wan', 'total_state_funding_az_pop_wan', 'total_erate_funding_az_pop_wan',
              'total_district_funding_az_pop_wan', 'builds_az_pop_wan', 'build_distance_az_pop_wan',
              'builds_ia', 'num_students_impacted'
              ]


os.chdir(GITHUB + '/Projects/sots-isl/data')
state_metrics.to_csv('id5134_ftg_state_metrics_2018_2018sots.csv', index=False)
print("File saved")


