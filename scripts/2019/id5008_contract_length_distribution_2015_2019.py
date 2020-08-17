##imports and definitions
from __future__ import division
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta


HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")


def graph_contract_dur(dataframe, limit=None, save_fig=False):
    filtered = dataframe.copy()
    total_contracts = len(filtered.frn)
    filtered['rounded_years'] = filtered.contract_len_years.map(round)

    # get contract length frequency for bar chart values
    counts = filtered[['frn', 'rounded_years']].groupby('rounded_years').count().reset_index()
    # group any contracts below 1 and above 10 for image
    c0 = counts[counts.rounded_years < 1].frn.sum()
    cmax = counts[counts.rounded_years > 10].frn.sum()

    cbar = counts.loc[(1 <= counts.rounded_years) & (counts.rounded_years < 10)].copy()
    cbar['rounded_years'] = cbar.rounded_years.map(str).copy()

    cbar = pd.DataFrame({'rounded_years': '<1', 'frn': c0}, index=[0]
                        ).append(cbar, ignore_index=True).append(
                {'rounded_years': '>10', 'frn': cmax}, ignore_index=True).copy()
    cbar['percent'] = cbar['frn'].map(lambda x: round(x/total_contracts*100, 1))

    xmarks = np.arange(0, cbar.shape[0])

    plt.figure(figsize=(12, 7))
    plt.bar(xmarks, cbar.frn, color='#009296')
    plt.xticks(xmarks, cbar.rounded_years)
    plt.yticks([])
    plt.box(on=None)
    plt.xlabel('Contract Length in Years')

    # if only a portion of the dataframe is used, label accurately
    if limit:
        plt.ylabel('Contracts {}'.format(limit))
    else:
        plt.ylabel('Total Contracts 2015-2019')

    # generate labels on bars directly with both counts and percentages
    for x0, y0, label, pct_label in zip(xmarks, cbar.frn, cbar.frn, cbar.percent):
        plt.text(x0, y0, str(label), ha='center', va='bottom', color='orange', weight='bold')
        plt.text(x0, y0+(cbar.frn.max()/40), str(pct_label)+'%', ha='center', va='bottom', weight='bold')

    # only save one figure due to limitations of ISL
    if save_fig:
        os.chdir(GITHUB + '/Projects/sots-isl/figure_images/')
        plt.savefig('id5008_contract_length_distribution_2015_2019.png')

    # return the values to be printed in the stdout but not saved anywhere
    return round(filtered.contract_len_years.mean(),2), round(filtered.contract_len_years.median(),2)


# connect to dar and save list of all clean for cost districts (excl. AK) with all of the relevant metrics/indicators
myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
queryfile = open('id5008_district_contracts.sql', 'r')
query = queryfile.read()
queryfile.close()

success = None
print ("Trying to establish initial connection to the server")
while success is None:
    conn = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DB)
    cur = conn.cursor()
    try:
        cur.execute(query)
        print("Success!")
        success = "true"
        break
    except psycopg2.DatabaseError:
        print('Server closed connection, trying again')
        pass
names = [x[0] for x in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=names)

myConnection.close()

# calculate and format contract length in days/years to be useful
df['service_start_date'] = pd.to_datetime(df['service_start_date'], errors='coerce').copy()
df['contract_expiration_date'] = pd.to_datetime(df['contract_expiration_date'], errors='coerce').copy()

filtered = df[~(df.service_start_date.isnull() | df.contract_expiration_date.isnull())].copy()

filtered['contract_len'] = filtered.apply(lambda row: relativedelta(row['contract_expiration_date'], row['service_start_date']), axis=1)
filtered['contract_len_days'] = filtered.contract_expiration_date - filtered.service_start_date
filtered['contract_len_years'] = filtered.contract_len_days.map(lambda x: round(int(x.days)/365, 2))

# to get chart with ALL data for all years distribution of contract length
# only generate figure file for this version, but have others for follow ups
mean, median = graph_contract_dur(filtered, save_fig=True)
print('All time: mean={}, meadian={}'.format(mean,median))

# to get metrics by year
for y in range(2015, 2020):

    mean, median = graph_contract_dur(filtered[filtered.funding_year == y], limit=y)
    print('{}: mean={}, median={}'.format(y, mean, median))

# get metrics by purpose
for i in filtered.purpose.unique():

    mean, median = graph_contract_dur(filtered[filtered.purpose == i], limit=i)
    print('{}: mean={}, meadian={}'.format(i, mean, median))

# get metrics by district_applied
for j in filtered.district_applied.unique():
    mean, median = graph_contract_dur(filtered[filtered.district_applied == j], limit=j)
    print('district_applied {}: mean={}, meadian={}'.format(j, mean, median))
