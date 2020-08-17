import psycopg2


def insert_value_db(df, table_short, HOST, USER, PASSWORD, DB):
    """Inserts into the isl_value_history_temp table or the isl_desc_history_temp table from a pandas DataFrame.
    Theses tables holds all Metric vales (if output type is value), or in the case of isl_desc_history_temp, ALL descriptions from the Master Google Sheet, and their input ids.
    Input Attributes: all are mandatory
        * **df**: input the pandas Dataframe
        * **table_short**: 'value' or 'desc'
        * **HOST,USER,PASSWORD,DB**: strings of your Postgres database credentials
    """
    myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB, port=5432)
    cursor = myConnection.cursor()

    tablename = 'isl_' + table_short + '_history_temp'

    cursor.execute("DROP TABLE IF EXISTS " + tablename + ";")

    if table_short == "value":
        column = 'value'
        create_statement = "CREATE TEMPORARY TABLE " + tablename + " (input_id int, value numeric);"
    else:
        column = 'description'
        create_statement = "CREATE TEMPORARY TABLE " + tablename + " (input_id int, description text);"

    cursor.execute(create_statement)

    for index, row in df.iterrows():

        cursor.execute("INSERT INTO " + tablename + " (input_id," + column + ") VALUES (%s,%s)",
                       (row['input_id'], row[column]))

    cursor.execute("SELECT dl.fn_isl_" + table_short + "_history();")

    cursor.close()
    myConnection.commit()
    myConnection.close()

    print("  values inserted")
