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
sys.path.insert(0, GITHUB + "/''scripts/2019/prework_queries")
from id5002_cost_projection_wide import *

HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")
conn = psy.connect(host=HOST_DAR, user=USER_DAR, password=PASSWORD_DAR, database=DB_DAR)


def main():
    os.chdir(GITHUB + '/''scripts/2019/prework_queries')
    circ_costs = get_data('id5001_median_circuit_costs.sql')

    combined = generate_cost_projections_wide(circ_costs)

    combined.to_csv(GITHUB+'''data/temp_curve.csv', index=False)

    condition = (combined.state == 'national') & ~(combined.circuit_size.isin(['$_mbps', 'aggregate']))
    plt.figure(figsize=(16, 12))

    colors = {'10000': 'darkblue',
             '1000': 'blue',
             '500': 'royalblue',
             '200': 'dodgerblue',
             '100': 'deepskyblue',
             '50': 'lightskyblue'}

    for c in combined[condition].circuit_size:
        y = combined.loc[condition & (combined.circuit_size == c),
                     'mrc_2016':'mrc_2025'].values[0].tolist()

        p_names = [n for n in combined.columns.tolist() if 'perc_decr_yoy_' in n]
        percs = combined.loc[condition & (combined.circuit_size == c), p_names].values[0].tolist()

        xdata = np.arange(2016, 2020)
        y0 = y[:4]

        plt.plot(xdata, y0, marker='o', color=colors[c])

        xnew = np.arange(2019, 2026)
        y1 = y[3:11]

        plt.plot(xnew, y1, '--', marker='o', color=colors[c], label=c)

        plt.legend(loc='upper right');
        y_pos = combined.loc[condition, 'mrc_2016'].values.tolist()
        y_vals = ['$' + str(round(n)) for n in combined.loc[condition, 'mrc_2016'].values.tolist()]
        plt.yticks(y_pos, y_vals);
        plt.xticks(np.arange(2016, 2026));

        dec = str(round(combined.loc[condition & (combined.circuit_size == c)].avg_decr.values[0]*-100, 2))+'%'
        plt.text(2020.1, y[4], dec, color='black', fontweight='bold')

        last = '$'+str(round(combined.loc[condition & (combined.circuit_size == c)].mrc_2025.values[0], 2))
        plt.text(2025.1, y[9], last, color='black')

    os.chdir(GITHUB + '/''figure_images')
    plt.savefig('id5004_circuit_cost_projected_2016_2025.png')


if __name__ == '__main__':
    main()
