import os
import psycopg2
import pandas as pd

HOST = os.environ.get("HOST_DAR")
USER = os.environ.get("USER_DAR")
PASSWORD = os.environ.get("PASSWORD_DAR")
DB = os.environ.get("DB_DAR")

# iniitalize a connection and a cursor
conn = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DB)
cur = conn.cursor()

# truncate history tables that will be repopulated
cur.execute("TRUNCATE TABLE dl.isl_value_history;")
cur.execute("TRUNCATE TABLE dl.isl_desc_history;")

# drop all of last year's history tables for individual scripts
tables_list_query = """
   SELECT quote_ident(table_schema) || '.'
        || quote_ident(table_name) AS table
   FROM information_schema.tables
   WHERE  table_name LIKE 'id' || '%' AND table_schema = 'dl'
"""

cur.execute(tables_list_query)
names = [x[0] for x in cur.description]
rows = cur.fetchall()
tables_list = pd.DataFrame(rows, columns=names)
# reassign as list
tables_list = tables_list['table'].values.tolist()

for table in tables_list:
    print(table)
    cur.execute("DROP TABLE " + table + ";")
    print("dropped")

cur.close()
conn.commit()
conn.close()
