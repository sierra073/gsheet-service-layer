##imports and definitions
import psycopg2
import os
import pandas as pd
import numpy as np
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import train_test_split
import statsmodels.api as sm
import math

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

# Lower cardinality of service providers, locale, size
df['primary_sp_short'] = np.where(np.logical_or(df['primary_sp'] == 'AT&T', df['primary_sp'] == 'Comcast'), 'att_comcast',
                                  np.where(df['primary_sp'] == 'Spectrum', 'spectrum', np.where(
                                      df['primary_sp'] == 'ENA Services', 'ena_services', np.where(
                                          df['primary_sp'] == 'CenturyLink', 'centurylink', np.where(
                                              df['primary_sp'] == 'Cox', 'cox', 'other')))))
df['locale_grouped'] = np.where(np.logical_or(df['locale'] == 'Rural', df['locale'] == 'Town'), 'Rural', df['locale'])
df['size_grouped'] = np.where(np.logical_or(df['size'] == 'Tiny', df['size'] == 'Small'), 'TinySmall',
                              np.where(np.logical_or(df['size'] == 'Large', df['size'] == 'Mega'), 'LargeMega',
                                       df['size']))

# Get dataset for regression
df_reg = df * 1
df_reg = df_reg[['num_students', 'locale_grouped', 'size_grouped', 'changed_contract_indicator', 'switched_sp', 'added_fiber', 'primary_sp_short', 'had_peer_deal_indicator', 'cost_decrease_indicator']]
df_reg = df_reg.dropna()

# convert dummies
cat_vars = ['locale_grouped', 'size_grouped', 'primary_sp_short']
for var in cat_vars:
    cat_list = 'var' + '_' + var
    cat_list = pd.get_dummies(df_reg[var], prefix=var)
    data1 = df_reg.join(cat_list)
    df_reg = data1

data_vars = df_reg.columns.values.tolist()
to_keep = [i for i in data_vars if i not in cat_vars]
data_final = df_reg[to_keep]

# final filters
data_final_vars = data_final.columns.values.tolist()
y = data_final['cost_decrease_indicator']
X = data_final[[i for i in data_final_vars if i not in 'cost_decrease_indicator']]
X = X.drop(['locale_grouped_Suburban', 'size_grouped_Medium', 'primary_sp_short_ena_services', 'num_students'], axis=1)

# implement logistic regression model
logit_model = sm.Logit(y.astype(float), X.astype(float))
result = logit_model.fit()
# print(result.summary())

ors = result.params.apply(lambda x: math.exp(x))
ors = ors.sort_values(ascending=False)
ors.name = 'Increase in Odds'
os.chdir(GITHUB + '/''data/')
ors[0:6].to_csv('id5004_districts_cost_decrease_chars_2018sots.csv', header=True)
