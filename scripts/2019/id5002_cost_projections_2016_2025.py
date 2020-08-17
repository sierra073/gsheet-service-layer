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

    melted_curv = cost_data_transform(combined)
    melted_curv.loc[melted_curv.year.map(int) >= 2020, 'perc_decr_yoy'] = melted_curv['avg_decr']
    melted_curv[['state',
                'circuit_size',
                'year',
                'avg_decr',
                'mrc',
                'perc_decr_yoy',
                'perc_decr']].to_csv(GITHUB+'''data/id5002_cost_projections_2016_2025.csv', index=False)


if __name__ == '__main__':
    main()
