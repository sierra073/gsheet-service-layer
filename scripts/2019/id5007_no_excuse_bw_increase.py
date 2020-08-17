##imports and definitions
import psycopg2
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")


def main():
    # connect to dar and save list of all clean for cost districts (excl. AK) with all of the relevant metrics/indicators
    myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

    cur = myConnection.cursor()

    os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019/prework_queries')
    queryfile = open('id5007_no_excuse_bw_increase.sql', 'r')
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

    # add new column for nearest circuit size to upgrade to
    ## Most popular circuits: 50 Mbps, 100 Mbps, 200 Mbps, 500 Mbps, 1 Gbps, 10 Gbps
    ## can input other circuits here:
    circuits = circuits = [0, 50] + [i*100 for i in range(1,10)] + [i*1000 for i in range(1,11)] + [20000]

    # add bw_increase_round_up column: rounds to the nearest circuit size (rounds up only)
    # function to find the closest circuit size needed to upgrade
    def find_closest_circuit_size_round_up(row):
        try:
            differences = row - np.array(circuits)
            min_circuit = np.nonzero(differences <= 0)[0][0]
            return circuits[min_circuit]
        except IndexError: # for bw needed over max circuits - 50000
            return 777777 # placeholder for anything over 50000

    df.loc[:, 'circuit_round_up'] = df.bw_diff.apply(find_closest_circuit_size_round_up)

    # another version
    # add bw_increase_closest column: rounds to the nearest circuit size (can round down or up)
    def find_closest_circuit_size(row):
        differences = abs(np.array(circuits) - row)
        min_diff = min(differences)
        bin_idx = max([i for i, d in enumerate(differences) if d == min_diff])
        return circuits[bin_idx]

    df.loc[:, 'circuit_closest'] = df.bw_diff.apply(find_closest_circuit_size)

    def derive_final_circuit_size(row):
        if (row.circuit_round_up != row.circuit_closest) and (abs(row.circuit_closest - row.bw_diff) < 20):
            return row.circuit_closest
        else:
            return row.circuit_round_up

    df['circuit_size'] = df.apply(derive_final_circuit_size, axis=1)
    df.loc[df.circuit_round_up != df.circuit_closest]
    df_counts = df[['district_id', 'circuit_size']].groupby('circuit_size').count().reset_index().rename(
        columns={'district_id': 'num_districts'})

    df_counts['circ_str'] = df_counts.circuit_size.map(lambda x:
                                                   str(x) if x < 1000
                                                   else str(round(x/1000)) + 'G'
                                                      )
    plt.figure(figsize=(12, 7))
    xmarks = [i for i in range(len(df_counts))]
    plt.bar(xmarks, df_counts.num_districts)

    plt.xticks(xmarks, df_counts.circ_str, rotation=20)
    plt.ylabel("Number of Districts")
    plt.yticks([])
    plt.box(on=None)

    # add sum labels
    for x0, v0, label in zip(xmarks, df_counts.num_districts, df_counts.num_districts):
        plt.text(x0, v0, round(label),ha='center', va='bottom', weight='bold')

    plt.axvline(x=12.3, color='orange', linestyle='--')
    plt.axvline(x=9, color='red', linestyle='--')
    plt.text(12.4, 58, 'mean={}'.format(round(df.bw_diff.mean(), 2)))
    plt.text(6.2, 58, 'median={}'.format(round(df.bw_diff.median())))

    os.chdir(GITHUB + '/Projects/sots-isl/figure_images')
    plt.savefig("id5007_no_excuse_bw_increase.png", bbox_inches = 'tight')


if __name__ == '__main__':
    main()
