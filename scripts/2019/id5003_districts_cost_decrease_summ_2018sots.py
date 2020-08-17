##imports and definitions
import psycopg2
import pandas as pd
import os

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

# connect to dar and save list of all clean for cost districts (excl. AK) with all of the relevant metrics/indicators
myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id5003_districts_cost_decrease_2018sots.sql', 'r')
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

# function to make individual summary tables with number and percent for an indicator


def make_indicator_summary(values_array):

    summ = pd.DataFrame({'metric': values_array[0],
                         'indicator': values_array[1],
                         'number': values_array[2],
                         'percent': values_array[3]}).reset_index(drop=True)
    summ = summ[['metric', 'indicator', 'number', 'percent']]

    return summ


# arrays with contents of the summary tables
num_districts = ['districts decreased cost/mbps?',
                 df['cost_decrease_indicator'].value_counts().index,
                 df['cost_decrease_indicator'].value_counts(),
                 df['cost_decrease_indicator'].value_counts(normalize=True)]
switchers = ['decreased cost: switched SP',
             df[df.cost_decrease_indicator == True]['switched_sp'].value_counts().index,
             df[df.cost_decrease_indicator == True]['switched_sp'].value_counts(),
             df[df.cost_decrease_indicator == True]['switched_sp'].value_counts(normalize=True)]
new_contract = ['decreased cost: got new contract',
                df[df.cost_decrease_indicator == True]['changed_contract_indicator'].value_counts().index,
                df[df.cost_decrease_indicator == True]['changed_contract_indicator'].value_counts(),
                df[df.cost_decrease_indicator == True]['changed_contract_indicator'].value_counts(normalize=True)]
added_fiber = ['decreased cost: upgraded to fiber',
               df[df.cost_decrease_indicator == True]['added_fiber'].value_counts().index,
               df[df.cost_decrease_indicator == True]['added_fiber'].value_counts(),
               df[df.cost_decrease_indicator == True]['added_fiber'].value_counts(normalize=True)]

final_summ = pd.DataFrame()

# Make final summary table with all metrics
for values_array in [num_districts, switchers, new_contract, added_fiber]:
    summ = make_indicator_summary(values_array)
    final_summ = pd.concat([final_summ, summ], axis=0, ignore_index=True)

final_summ = final_summ.sort_values(['metric', 'indicator'], ascending=[False, False])

os.chdir(GITHUB + '/''data/')
final_summ.to_csv('id5003_districts_cost_decrease_summ_2018sots.csv', index=False)
