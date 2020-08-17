import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import os

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")
GITHUB = os.environ.get("GITHUB")

myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)

cur = myConnection.cursor()

os.chdir(GITHUB + '/''scripts/2019/prework_queries')
queryfile = open('id9003_1mbps_path_states_donut_v2.sql', 'r')
query = queryfile.read()
queryfile.close()

cur.execute(query)

names = [x[0] for x in cur.description]
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=names)

# create data
df = df.groupby('category').count().reset_index()
df.columns = ['category', 'num_states']
size_of_groups = df['num_states'].values.tolist()
print(size_of_groups)

colors = ["#004f51", "#009296", "#bfe6ef", "#6acce0"]

plt.rc('figure', figsize=(8, 8))

fig, ax = plt.subplots()
wedges, _ = ax.pie(size_of_groups, startangle=90, counterclock=False, colors=colors)

plt.setp(wedges, width=0.3)

ax.set_aspect("equal")

# save image
os.chdir(GITHUB + '/''figure_images')
plt.savefig('id9003_1mbps_path_states_donut.png', transparent=True)
