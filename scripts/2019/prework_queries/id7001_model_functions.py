from __future__ import division
import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 
import matplotlib.font_manager as fm

def find_circuit_group(bandwidth):
    if bandwidth >= 10000:
        return '10000'
    elif bandwidth >= 1000:
        return '1000'
    elif bandwidth >= 500:
        return '500'
    elif bandwidth >= 200:
        return '200'
    elif bandwidth >= 100:
        return '100'
    else:
        return '50'

def cost_data_transform(cost_data):
    cost_data['value'] = abs(cost_data['perc_decr'])
    cost_data = cost_data[cost_data['year'] > 2019].copy()
    return cost_data

def find_best_peer_deal(cost_data, peer_data, cost_projection_type, cost_group, same_sp_in_contract, same_sp_preference):

    ##cost_projection_type = 'circuit','$/Mbps','current_pricing'
    ##cost_group = 'national','state' - don't end up actually using state but wanted to keep functionality
    ##same_sp_in_contract = True/False - can a district upgrade within their current contract as long as the peer deal is with current provider
    ##same_sp_preference = True/False - pick peer deals from current provider over those from different provider, regardless of cost
    
    ## convert columns 
    peer_data['primary_new_contract_start_date'] = peer_data['primary_new_contract_start_date'].astype(int)
    numeric_cols = ['primary_ia_monthly_cost_total','match_ia_bw_mbps_total','match_ia_monthly_cost_total','match_ia_monthly_cost_per_mbps']
    peer_data[numeric_cols] = peer_data[numeric_cols].astype(float)
    cost_data['year'] = cost_data['year'].astype(int)
    cost_data['value'] = cost_data['value'].astype(float)

    ## define whether to use national medians or state medians for price projections
  	## originally going to use AK medians for AK with National, but sample size too small for AK data
    if cost_group == 'national':
        ##peer_data['cost_group'] = np.where(peer_data['match_state']=='AK', peer_data['match_state'], 'national')
        peer_data['cost_group'] = 'national'
    elif cost_group == 'state':
        peer_data['cost_group'] = peer_data['match_state']

    ## merge peer deal data and cost projection data 
    if cost_projection_type == '$/mbps': 
        peer_data_combo = pd.merge(peer_data,cost_data[(cost_data['circuit_size']=='$_mbps')], left_on = 'cost_group',right_on = 'state')
    elif cost_projection_type == 'circuit':
        peer_data['circuit_group'] = peer_data['match_ia_bw_mbps_total'].apply(find_circuit_group)
        peer_data_combo = pd.merge(peer_data,cost_data,left_on = ['cost_group','circuit_group'],right_on= ['state','circuit_size'])
    elif cost_projection_type == 'current_pricing':
        peer_data_combo = peer_data.copy()
        peer_data_combo['year'] = 2019
    
    peer_data_combo.rename(columns={'year':'peer_year'},inplace = True)

    ## apply cost projections to peer deals or else use 2019 pricing
    if cost_projection_type == 'current_pricing':
        peer_data_combo['match_projected_cost'] = peer_data_combo['match_ia_monthly_cost_total']
    else: 
        peer_data_combo['match_projected_cost'] = peer_data_combo['match_ia_monthly_cost_total'] - (peer_data_combo['match_ia_monthly_cost_total']*peer_data_combo['value'])
    
    ## limit to deals primary district can afford
    peer_data_combo = peer_data_combo[peer_data_combo['match_projected_cost'] <= peer_data_combo['primary_ia_monthly_cost_total']].copy()

    ## figure out what year the district will upgrade
    ## dependent on their contract end date
    ## when prices go down enough they can afford it 
    ## and/or whether they can do it within their current contract if they keep their service provider
    if cost_projection_type == 'current_pricing':
        if same_sp_in_contract == True:
            peer_data_combo['actual_year'] = np.where(peer_data_combo['same_primary_sp']==True,2020,peer_data_combo['primary_new_contract_start_date'])

        elif same_sp_in_contract == False:
            peer_data_combo['actual_year'] = peer_data_combo['primary_new_contract_start_date']


    else:
        if same_sp_in_contract == True:
            peer_data_combo['actual_year'] = np.where(peer_data_combo['same_primary_sp']==True,peer_data_combo['peer_year'],peer_data_combo['primary_new_contract_start_date'])

        elif same_sp_in_contract == False:
            peer_data_combo['actual_year'] = peer_data_combo['primary_new_contract_start_date']
        
        peer_data_combo['actual_year'] = peer_data_combo['actual_year'].astype(int)
        peer_data_combo['peer_year'] = peer_data_combo['peer_year'].astype(int)
        peer_data_combo = peer_data_combo[peer_data_combo['actual_year'] <= peer_data_combo['peer_year']].copy()
        peer_data_combo = peer_data_combo.groupby(['district_id','num_students','primary_ia_monthly_cost_total','actual_year','primary_new_contract_start_date',
        'peer_id','same_state','same_primary_sp','match_service_provider'],as_index=False).agg({'peer_year':min,'match_projected_cost':max})
        peer_data_combo['actual_year'] = peer_data_combo['peer_year']

    ## rank deals to find the best one for district
    ## dependent on what is the earliest and what is either the cheapest or cheapest from current provider 
    peer_data_combo['year_rank'] = peer_data_combo.groupby(['district_id'])['actual_year'].rank(ascending=1,method='dense')
    ## forcing it to be as early as possible - change if we want same sp preference to exist outside of years
    peer_data_combo = peer_data_combo[peer_data_combo['year_rank']==1].copy()

    if same_sp_preference == True:
        peer_data_combo['same_primary_sp_bool'] = np.where(peer_data_combo['same_primary_sp']==True,1,0)
        peer_data_combo['same_sp_rank'] = peer_data_combo.groupby(['district_id'])['same_primary_sp_bool'].rank(ascending=0,method='dense')
        peer_data_combo['cost_rank'] = peer_data_combo.groupby(['district_id','year_rank','same_sp_rank'])['match_projected_cost'].rank(ascending=1,method='dense')
        best_peer = peer_data_combo[(peer_data_combo['same_sp_rank']==1)&(peer_data_combo['cost_rank']==1)&(peer_data_combo['year_rank']==1)].copy()
    
    elif same_sp_preference == False:
        peer_data_combo['cost_rank'] = peer_data_combo.groupby(['district_id','year_rank'])['match_projected_cost'].rank(ascending=1,method='dense')
        best_peer = peer_data_combo[(peer_data_combo['cost_rank']==1)&(peer_data_combo['year_rank']==1)].copy()
    best_peer['upgrade_within_contract'] = best_peer['actual_year'] < best_peer['primary_new_contract_start_date']
    best_peer['cost_diff'] = best_peer['primary_ia_monthly_cost_total'] - best_peer['match_projected_cost']

    best_peer = best_peer[['district_id','num_students','actual_year','peer_year','primary_new_contract_start_date','same_primary_sp','upgrade_within_contract','peer_id','match_projected_cost','cost_diff']]

    return best_peer

def plot_1mbps_future_model(cost_data, peer_data, extrap_data, cost_projection_type, cost_group, same_sp_in_contract, same_sp_preference, units):
    ## convert columns & get data
    extrap_data_mod = extrap_data.copy()
    extrap_data_mod['primary_new_contract_start_date'] = extrap_data_mod['primary_new_contract_start_date'].astype(int)
    best_peer_data = find_best_peer_deal(cost_data, peer_data, cost_projection_type, cost_group, same_sp_in_contract, same_sp_preference)
    ## remove duplicate peer deals where every thing is same except peer id 
    best_peer_data.drop(columns=['peer_id'],inplace=True)
    best_peer_data.drop_duplicates(keep = 'first', inplace = True)

    if units == 'districts':
        best_peer_data['units'] = 1

    elif units == 'students':
        best_peer_data['units'] = best_peer_data['num_students']
        
        def convert_students(df, column):
            df[column] = np.where(df[column]==1,df['num_students'],0)

        columns_units_convert = ['clean_path_1m_sample','not_meeting','meeting','sample','population']
        for i in columns_units_convert:
            convert_students(extrap_data_mod,i)

    ## aggregate districts in the "Pay More" group
    pay_more = extrap_data_mod[~extrap_data_mod['district_id'].isin(best_peer_data['district_id'])].groupby('primary_new_contract_start_date',as_index=False)['clean_path_1m_sample'].sum()
    pay_more.rename(columns={'primary_new_contract_start_date':'actual_year','clean_path_1m_sample':'units'},inplace=True)
    pay_more['group'] = 'Y'
    pay_more['units'] 
    pay_more = pay_more[['group','actual_year','units']]

    ## aggregate peer_data districts by groups
    best_peer_data['group'] = np.where(best_peer_data['same_primary_sp']==False,'A',
        np.where((best_peer_data['same_primary_sp']==True)&(best_peer_data['upgrade_within_contract']==False),'B',
            np.where((best_peer_data['same_primary_sp']==True)&(best_peer_data['upgrade_within_contract']==True),'C','error')))
    best_peer_summ = best_peer_data.groupby(['group','actual_year'],as_index=False)['units'].sum()
    
    clean_path_districts = pd.concat([best_peer_summ,pay_more],sort=False)

    ## create dataframe of all years within range
    years_range = range(2019,max(clean_path_districts['actual_year']+1))
    group_list = list(np.unique(clean_path_districts['group']))
    all_years_df = pd.DataFrame(columns=['group','actual_year'])
    for i in group_list:
        y = [i]*len(years_range)
        data = pd.DataFrame({'group':y, 'actual_year':list(years_range)})
        all_years_df = all_years_df.append(data)
    all_years_df.reset_index(drop=True,inplace=True)
    all_years_df['actual_year'] = all_years_df['actual_year'].astype(int)
    all_years_df = pd.merge(all_years_df,clean_path_districts,on=['group','actual_year'],how='left').fillna(0)
    all_years_df['cumulative_units'] = all_years_df.groupby('group')['units'].cumsum()

    ## extrapolations overall 
    already_meeting_p = (sum(extrap_data_mod['meeting'])/sum(extrap_data_mod['sample']))
    already_meeting_extrap = already_meeting_p*sum(extrap_data_mod['population'])
    not_meeting_p = (sum(extrap_data_mod['not_meeting'])/sum(extrap_data_mod['sample']))
    not_meeting_extrap = not_meeting_p*sum(extrap_data_mod['population'])

    ## extrapolations for each group
    all_years_extrap = all_years_df.groupby(['group'],as_index=False)['units'].sum()
    all_years_extrap.rename(columns={'units':'total_units'},inplace=True)
    all_years_extrap['total_units_p'] = all_years_extrap['total_units']/sum(extrap_data_mod['clean_path_1m_sample'])
    all_years_extrap['total_units_extrap'] = all_years_extrap['total_units_p']*not_meeting_extrap

    ## extrapolations for each year for each group
    all_years_df = pd.merge(all_years_df,all_years_extrap,on ='group',how='left')
    all_years_df['units_extrap'] = (all_years_df['units']/all_years_df['total_units'])*all_years_df['total_units_extrap']
    all_years_df['cumulative_units_extrap'] = all_years_df.groupby('group')['units_extrap'].cumsum()
    
    ## add already meeting group
    already_meeting_df = pd.DataFrame({'group':['Z']*len(years_range), 'actual_year':list(years_range)})
    already_meeting_df['units_extrap'] = np.where(already_meeting_df['actual_year']==2019,already_meeting_extrap,0)
    already_meeting_df['cumulative_units_extrap'] = already_meeting_extrap

    all_years_df = pd.concat([all_years_df[['group','actual_year','units_extrap','cumulative_units_extrap']],already_meeting_df],sort=False)

    waterfall_df = all_years_df.pivot_table(index='actual_year',columns='group',values='units_extrap').reset_index()
    
    waterfall_df_totals = pd.DataFrame(waterfall_df.sum(axis=0),columns=['Total']).T

    waterfall_df['Y'] = 0
    waterfall_df['row_total'] = waterfall_df.iloc[:,1:].sum(axis=1)
    waterfall_df['starting_point'] = waterfall_df['row_total'].cumsum().shift(1).fillna(0)

    waterfall_df_totals['actual_year'] = 'Total'
    waterfall_df_totals['row_total'] = waterfall_df_totals.iloc[:,1:].sum(axis=1)
    waterfall_df_totals['starting_point'] = 0

    waterfall_df = pd.concat([waterfall_df,waterfall_df_totals],sort=False)

    waterfall_df['start_Z'] = waterfall_df['starting_point']
    waterfall_df['start_B'] = waterfall_df['start_Z'] +  waterfall_df['Z']
    if same_sp_in_contract == True:
        waterfall_df['start_C'] = waterfall_df['start_B'] + waterfall_df['B']
        waterfall_df['start_A'] = waterfall_df['start_C'] + waterfall_df['C']
    else:
        waterfall_df['start_A'] = waterfall_df['start_B'] + waterfall_df['B']
    waterfall_df['start_Y'] = waterfall_df['start_A'] + waterfall_df['A']

    waterfall_df['year_filter'] = np.where(waterfall_df['actual_year']=='Total',0,waterfall_df['actual_year'])
    waterfall_df_all_data = waterfall_df.copy()
    waterfall_df = waterfall_df[waterfall_df['year_filter']<2025].copy()
    names = list(waterfall_df['actual_year'])
    barWidth = .9
    r = range(0,len(names))
    
    fig = plt.figure(figsize=(8,6),dpi=200)
    plt.rcParams['font.family'] = 'Lato'
    plt.xticks(r,names)
    plt.xlabel('Upgrade Year')
    sns.set_style('white', {'axes.linewidth': .5, 'axes.edgecolor':'#A1A1A1'})
    plt.gca().spines['top'].set_color('none')
    plt.gca().spines['right'].set_color('none')

    pZ = plt.bar(r, waterfall_df['Z'], bottom=waterfall_df['start_Z'] ,color='#cccccc', edgecolor='white', width=barWidth)
    pB = plt.bar(r, waterfall_df['B'], bottom=waterfall_df['start_B'] ,color='#6acce0', edgecolor='white', width=barWidth)
    pA = plt.bar(r, waterfall_df['A'], bottom=waterfall_df['start_A'] ,color='#006b6e', edgecolor='white', width=barWidth)
    pY = plt.bar(r, waterfall_df['Y'], bottom=waterfall_df['start_Y'] ,color='#cb2128', edgecolor='white', width=barWidth)

    A = 'Peer Deal - Diff. Provider'
    if same_sp_in_contract == True: 
        B = 'Peer Deal - Current Provider New Contract'
    else:
        B = 'Peer Deal - Current Provider'
    C = 'Peer Deal - Current Provider Current Contract'
    Y = 'Pay More'
    Z = 'Already Meeting'

    if same_sp_in_contract == True:
        pC = plt.bar(r, waterfall_df['C'], bottom=waterfall_df['start_C'] ,color='#f26c23', edgecolor='white', width=barWidth)
        plt.legend((pY,pB,pC,pA,pZ),(Y,B,C,A,Z),loc='lower right')
    else:
        plt.legend((pY,pB,pA,pZ),(Y,B,A,Z),loc='lower right')

    if units == 'districts':
        plt.ylabel('Districts')
        label_y_shift = 100
    elif units == 'students':
        plt.ylabel('Students')
        label_y_shift = 1000
    label_x_shift = .35

    waterfall_df['bar_total'] = waterfall_df['row_total']+waterfall_df['starting_point']

    for count,year in enumerate(names):
        plt.text(x=count-label_x_shift, y = waterfall_df.iloc[count]['bar_total'] + label_y_shift, 
             s = "{:0,.0f} ({:.0f}%)".format(round(waterfall_df.iloc[count]['bar_total']),round((waterfall_df.iloc[count]['bar_total']/waterfall_df.loc['Total']['row_total'])*100)),
             size = 7)
    
    if (waterfall_df.loc['Total']['Y']/waterfall_df.loc['Total']['row_total']) >= .05:
        plt.text(x=6-label_x_shift,y = waterfall_df.loc['Total']['start_Y'] +waterfall_df.loc['Total']['Y']/2-label_y_shift,
            s = "{:0,.0f} ({:.0f}%)".format(round(waterfall_df.loc['Total']['Y']),round((waterfall_df.loc['Total']['Y']/waterfall_df.loc['Total']['row_total'])*100)),
             size = 7,color = 'white')
    else:
        plt.text(x=6-label_x_shift,y = waterfall_df.loc['Total']['start_Y'] +waterfall_df.loc['Total']['Y']/2-label_y_shift,
            s = "{:0,.0f} ({:.0f}%)".format(round(waterfall_df.loc['Total']['Y']),round((waterfall_df.loc['Total']['Y']/waterfall_df.loc['Total']['row_total'])*100)),
             size = 7,color = 'black')

    ## clean up dataframe
    if same_sp_in_contract == True: 
        waterfall_df_all_data['Peer Combo'] = waterfall_df_all_data['A'] + waterfall_df_all_data['B'] + waterfall_df_all_data['C']
        int_columns = ['Peer Combo','A','B','C','Y','Z','row_total']
    else:
        waterfall_df_all_data['Peer Combo'] = waterfall_df_all_data['A'] + waterfall_df_all_data['B']
        int_columns = ['Peer Combo','A','B','Y','Z','row_total']
    waterfall_df_all_data = waterfall_df_all_data[['actual_year'] + int_columns].copy()
    waterfall_df_all_data[int_columns] = waterfall_df_all_data[int_columns].apply(pd.Series.round).astype(int)
    waterfall_df_all_data.rename(columns={'A':A,'B':B,'C':C,'Y':Y,'Z':Z},inplace=True)

    return fig, waterfall_df_all_data