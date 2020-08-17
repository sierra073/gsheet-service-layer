import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import psycopg2 as psy

from dotenv import load_dotenv, find_dotenv
from scipy.optimize import curve_fit

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")

HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
conn = psy.connect(host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR)


def main():
    os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
    circ_costs = get_data('id5001_median_circuit_costs.sql')

    df = circ_costs[['funding_year', 'state', 'circuit_size', 'median_rec_cost']]
    df.median_rec_cost = df.median_rec_cost.map(float)

    #manually hard-coding values from ONYX
    df.loc[(df.funding_year == 2015) & (df.state == 'national') & (df.circuit_size == '10000'),
           'median_rec_cost'] = 10192
    df.loc[(df.funding_year == 2016) & (df.state == 'national') & (df.circuit_size == '10000'),
           'median_rec_cost'] = 8972.95

    trans = df.set_index(['state', 'circuit_size', 'funding_year']).unstack('funding_year').reset_index()
    trans.columns = trans.columns.map('{0[0]}_{0[1]}'.format).str.strip('_')
    trans.columns = trans.columns.str.replace('median_rec_cost', 'mrc')

    # predict circuit-level mrc
    nat = trans[(trans.circuit_size != '$_mbps')]
    for n in range(2016, 2020):
        nat['perc_decr_yoy_{}'.format(n)] = ((nat['mrc_{}'.format(n)] -
                                              nat['mrc_{}'.format(n-1)])/
                                              nat['mrc_{}'.format(n-1)])

    nat['avg_decr'] = nat[['perc_decr_yoy_2016', 'perc_decr_yoy_2017',
                           'perc_decr_yoy_2018', 'perc_decr_yoy_2019']].mean(axis=1).map(abs)

    # multiply all by avg decr from 15 to now
    for n in range(2020, 2026):
        nat['mrc_{}'.format(n)] = nat['mrc_{}'.format(n-1)] - nat['mrc_{}'.format(n-1)]*nat['avg_decr']

    curve = project_costs_curve(trans[trans.circuit_size == '$_mbps'].reset_index(drop=True))
    combined = pd.concat([curve, nat])

    for n in range(2020, 2026):
        combined['perc_decr_{}'.format(n)] = ((combined['mrc_{}'.format(n)] -
                                          combined.mrc_2019)/
                                          combined.mrc_2019)

    melted_curv = cost_data_transform(combined)
    melted_curv.loc[melted_curv.year.map(int) >= 2020, 'perc_decr_yoy'] = melted_curv['avg_decr']
    melted_curv[['state',
                'circuit_size',
                'year',
                'avg_decr',
                'mrc',
                'perc_decr_yoy',
                'perc_decr']].to_csv(GITHUB+'/Projects/sots-isl/data/id5001_cost_projections_2015_2025.csv', index=False)


def get_data(sql_file):
    cur = conn.cursor()
    cur.execute(open(sql_file, "r").read())
    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=names)


def pchange_2019(v2019, vyear):
    return str(round(((vyear-v2019)/v2019)*100, 2))+'%'


def func(x, a, b, c):
    return a * np.exp(-b * x) + c


def linfunc(x, a, b):
    return a - b * x


def cost_data_transform(cost_data):
    ##fix percent decreases to be from original 2019 cost and not year over year
    cost_data = pd.melt(cost_data, id_vars = ['state','circuit_size', 'avg_decr'],
        var_name = 'cost_type',
        value_name = 'value')
    cost_data['year']= cost_data.cost_type.str.rsplit('_', 1).str[1]
    cost_data['type'] = cost_data.cost_type.str.rsplit('_', 1).str[0]

    mrc = cost_data.loc[cost_data.type == 'mrc',
                       ['state','circuit_size', 'year', 'avg_decr', 'value']
                       ].rename(columns={'value': 'mrc'})
    perc_decr_yoy = cost_data.loc[cost_data.type == 'perc_decr_yoy',
                                  ['state','circuit_size', 'year', 'avg_decr', 'value']
                                 ].rename(columns={'value': 'perc_decr_yoy'})
    perc_decr = cost_data.loc[cost_data.type == 'perc_decr', ['state','circuit_size', 'year', 'avg_decr', 'value']].rename(columns={'value': 'perc_decr'})

    cost_data = mrc.merge(perc_decr_yoy, on=['state','circuit_size', 'year', 'avg_decr'], how='outer'
                 ).merge(perc_decr, on=['state','circuit_size', 'year', 'avg_decr'], how='outer')
    return cost_data


def project_costs_curve(dataframe):
    xdata = np.arange(0, 5)
    for i in range(len(dataframe)):
        y = list(dataframe.loc[i, 'mrc_2015':'mrc_2019'])
        x_new = np.arange(0, 11)

        try:
            popt, pcov = curve_fit(func, xdata, y, maxfev=1500)
            projected_vals = func(x_new, *popt)
            v2019 = func(x_new, *popt)[4]
        except ValueError:
            projected_vals = [0 for i in range(10)]
        for n in range(6):
            dataframe.loc[i, 'mrc_{}'.format(2020 + n)] = projected_vals[5 + n]

    for n in range(2016, 2020):
        dataframe['perc_decr_yoy_{}'.format(n)] = ((dataframe['mrc_{}'.format(n)] -
                                                      dataframe['mrc_{}'.format(n-1)])/
                                                      dataframe['mrc_{}'.format(n-1)])

    dataframe['avg_decr'] = dataframe.loc[:, 'perc_decr_yoy_2016':'perc_decr_yoy_2019'].mean(axis=1).map(abs)
    return dataframe


if __name__ == '__main__':
    main()
