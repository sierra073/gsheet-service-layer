import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import psycopg2 as psy

from dotenv import load_dotenv, find_dotenv
from scipy.optimize import curve_fit
import sys

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")
sys.path.insert(0, GITHUB + "/Projects/sots-isl/scripts/2019/prework_queries")
from id5002_cost_projection_wide import *

HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
conn = psy.connect(host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR)


def main():
    os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
    circ_costs = get_data('id5001_median_circuit_costs.sql')

    combined = generate_cost_projections_wide(circ_costs)

    combined.to_csv(GITHUB+'Projects/sots-isl/data/temp_curve.csv', index=False)

    condition = (combined.state == 'national') & (combined.circuit_size == '$_mbps')
    y = combined.loc[condition,
                'mrc_2016':'mrc_2025'].values[0].tolist()

    for n in range(2020, 2026):
        combined['perc_decr_yoy_{}'.format(n)] = ((combined['mrc_{}'.format(n)] -
                                                  combined['mrc_{}'.format(n-1)])/
                                                  combined['mrc_{}'.format(n-1)])

    p_names = [n for n in combined.columns.tolist() if 'perc_decr_yoy_' in n]
    percs = combined.loc[condition, p_names].values[0].tolist()

    plt.figure(figsize=(12, 7))
    xdata = np.arange(2016, 2020)
    y0 = y[:4]

    plt.plot(xdata, y0, marker='o', color='orange', label='national $/mbps')

    xnew = np.arange(2019, 2026)
    y1 = y[3:11]

    plt.plot(xnew, y1, '--', marker='o', color='orange', label='projected $/mbps')

    plt.legend(loc='upper right');
    y_pos = np.arange(8)
    y_vals = ['$' + str(n) for n in range(8)]
    plt.yticks(y_pos, y_vals);
    plt.xticks(np.arange(2016, 2026));

    for i, p in enumerate(zip(np.arange(2017, 2026), y[1:], percs)):
        plt.text(p[0], p[1]+.1, str(round(p[2]*100))+'%', color='black', fontweight='bold')

    os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
    plt.savefig('id5003_cost_per_mbps_projected_2016_2025.png')


if __name__ == '__main__':
    main()
