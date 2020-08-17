import os
import pandas as pd
import psycopg2
from datetime import datetime
import time
from dotenv import load_dotenv, find_dotenv
from read_from_google_sheet import get_master_as_dict
from generate_history_value import insert_value_db
from generate_history_other import upload_hist, insert_table_hist

global HOST_DAR_HIST, USER_DAR_HIST, PASSWORD_DAR_HIST, DB_DAR_HIST
HOST_DAR_HIST = "ec2-18-205-200-121.compute-1.amazonaws.com"
USER_DAR_HIST = os.environ.get("USER_DAR_HIST")
PASSWORD_DAR_HIST = os.environ.get("PASSWORD_DAR_HIST")
DB_DAR_HIST = os.environ.get("DB_DAR_HIST")

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")


def generate_output(insight, input_id, scriptname, type_of_output, output, rownum):
    """Takes in the ``insight`` ('Metric/Insight'), ``input_id``, ``scriptname`` (the name of the script without extension), ``type_of_output`` ('input_format_of_output'), the output itself, and the row number ``rownum`` of the Master Master Google Sheet (first 'Metrics' sheet).
    Populates the cell corresonding to (``rownum``,'output') (9th column in Master Master) with the output in the format specified.
    """
    mm_sheet, mm_dict = get_master_as_dict("Master Metric Tracker - SotS 2019")

    if type_of_output == 'value':
        if output.shape[0] > 1:
            print("  Unable to populate value -- should be a table")
            return

        time.sleep(2)
        mm_sheet.update_cell(rownum, 9, str(output.iloc[0].item()))

        # output history
        value_df = pd.DataFrame({'input_id': input_id, 'value': output.iloc[0].item()}, index=[0])
        insert_value_db(value_df, 'value', HOST_DAR_HIST, USER_DAR_HIST, PASSWORD_DAR_HIST, DB_DAR_HIST)
        # output link to history tableau
        hist_link = 'https://10az.online.tableau.com/#/site/eshtableau/views/ISLValuesHistory/ISLValuesOverTimebyinput_id?input_id=' + str(input_id)
        print('  ' + hist_link)
        mm_sheet.update_cell(rownum, 10, '=HYPERLINK("' + hist_link + '", "history visual")')

    elif type_of_output == 'table':
        if len(output) > 0:
            folder_id, link = upload_hist(scriptname, type_of_output, '12BnY4QAbX1BYbO8WgBQ3-JipUYHTd2kf')
        else:
            return

        # output a link to csv
        mm_sheet.update_cell(rownum, 9, '=HYPERLINK("' + link + '", "table")')
        # output link to history folder
        hist_link = "https://drive.google.com/drive/u/0/folders/" + folder_id
        mm_sheet.update_cell(rownum, 10, '=HYPERLINK("' + hist_link + '", "history folder")')

        # output history
        # drop duplicate columns
        # output = output.loc[:, ~output.columns.duplicated()]
        # output['input_id'] = input_id
        # output['create_date'] = datetime.now()
        # df_exists = get_latest_from_db(input_id, 'check_table_exists.sql', HOST_DAR, USER_DAR, PASSWORD_DAR, DB_DAR)
        # insert_table_hist(output, df_exists, scriptname, HOST_DAR, USER_DAR, PASSWORD_DAR, DB_DAR)

    elif type_of_output == 'figure':
        # output history
        if len(output) > 1:
            folder_id, link = upload_hist(output, type_of_output, '1u-u_7tWYISsdApaOWx5pw1zXCOvQa0LU')
        else:
            return
        # output a link to figure
        mm_sheet.update_cell(rownum, 9, '=HYPERLINK("' + link + '", "figure linked")')
        # output link to history folder
        hist_link = "https://drive.google.com/drive/u/0/folders/" + folder_id
        mm_sheet.update_cell(rownum, 10, '=HYPERLINK("' + hist_link + '", "history folder")')

    print('  output successful') + '\n' + '******************************'


def get_latest_from_db(input_id, file, HOST, USER, PASSWORD, DB):

    os.chdir(GITHUB + '/Projects/sots-isl/src/history_sql/')
    queryfile = open(file, 'r')
    query = queryfile.read()
    queryfile.close()
    # replace placeholder with input_id
    query = query.replace('x', str(input_id))

    conn = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DB)
    cur = conn.cursor()
    cur.execute(query)

    names = [x[0] for x in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=names)

    conn.close()

    return df


def output_change_to_description(mm_sheet, rownum, insight, input_id, HOST, USER, PASSWORD, DB):

    latest_desc = get_latest_from_db(input_id, 'get_description.sql', HOST, USER, PASSWORD, DB)

    if (not latest_desc.empty) and (insight != latest_desc['description'].item()):
        mm_sheet.update_cell(rownum, 11, 'Description changed, check Metric/Insight')
