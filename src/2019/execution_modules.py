import os
import psycopg2
import pandas as pd
import re
import subprocess
from dotenv import load_dotenv, find_dotenv

# get the GitHub (ficher) path base
global GITHUB
load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")


def run_sql(filepath, HOST, USER, PASSWORD, DB):
    """Creates a pandas DataFrame with the result from a query to a Postgres database.
    Input Attributes: all are mandatory
        * **filepath**: input the path of the sql file as a string, e.g. *'src/sql/get_raw_data.sql'*
        * **HOST,USER,PASSWORD,DB**: strings of your Postgres database credentials
    """
    os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019')
    try:
        queryfile = open(filepath, 'r')
    except IOError:
        print("  File not found -- please check file name (input_filepath)")
        print('******************************')
        return

    print("  Querying data from DB connection")
    queryfile = open(filepath, 'r')
    query = queryfile.read()
    queryfile.close()

    success = None
    count = 0
    print ("  Trying to establish initial connection to the server...")
    while success is None:
        conn = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DB)
        cur = conn.cursor()
        try:
            cur.execute(query)
            print("    ...Success!")
            success = "true"
            names = [x[0] for x in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=names)
            return df
        except psycopg2.DatabaseError:
            print('  Server closed connection, trying again')
            if count == 10:
                print('  Please fix query logic')
                raise Exception('  Please fix query logic')
                print('******************************')
            else:
                count += 1
                pass

    conn.close()


def run_script(filepath, software, format):
    """Creates a pandas DataFrame with the result from running a script (Python or R).
    If the **format** is a value, it's implied that the script will print it.
    If the **format** is a table, it's implied that the script will write a csv to ``sots-isl/data/``.
    If the **format** is a figure, this will just run the script to generate it and return the name of the figure file.
    Input Attributes: all are mandatory
        * **filepath**: input the path of the sql file as a string, e.g. *'src/sql/get_raw_data.sql'*
        * **software**: 'python' or 'r'
        * **format**: format of output. 'value', table', or 'figure'
    """

    # checking if file exists
    scriptpath = GITHUB + '/Projects/sots-isl/scripts/2019/' + filepath
    try:
        file = open(scriptpath, 'r')
    except IOError:
        print("  File does not exist: " + str(scriptpath))
        print('******************************')
        return pd.DataFrame()

    if software == 'python':
        cmd = 'python ' + scriptpath
    else:
        cmd = 'Rscript ' + scriptpath

    # error handling - check script contents match format
    scripttext = open(scriptpath).read()

    if format == 'value' and ('print' not in scripttext):
        print("  Error: script for value output format requires printing, and only once")
        return
    if format == 'table' and ('csv(' not in scripttext):
        print("  Error: script for table output format requires writing to csv")
        return
    if format == 'figure' and (not find_substring_within_quotes('PNG', scripttext)):
        print("  Error: script for figure output format requires PNG")
        return

    # remove csvs and pngs from prior runs (tables and figures)
    if format == 'table':
        try:
            os.remove(GITHUB + '/Projects/sots-isl/data/' + filepath.split('.')[0] + '.csv')
        except OSError:
            pass
    if format == 'figure':
        try:
            os.remove(GITHUB + '/Projects/sots-isl/figure_images/' + filepath.split('.')[0] + '.png')
        except OSError:
            pass

    # executing script
    a = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    print('  executed')
    print('******************************')

    # returns output as a dataframe
    if format == 'value':
        valstring = a.communicate()[0].decode('utf-8')

        if software == 'python':
            try:
                val = float(valstring)
            except ValueError:
                print("  Error in script, file may not exist")
                return valstring
        elif software == 'R':
            try:
                val = valstring.split('] ')[-1]
                if val.endswith("\n") or val.endswith("\r"):
                    val = float(val[:-1])
                else:
                    val = float(val)
            except ValueError:
                print("  Error in script, file may not exist")
                return

        df = pd.DataFrame({'value': val}, index=[0])

    elif format == 'table':

        a.communicate()
        try:
            df = pd.read_csv(GITHUB + '/Projects/sots-isl/data/' + filepath.split('.')[0] + '.csv')
        except OSError:
            print("  Table csv not found, please check script or input_filepath")
            return

    elif format == 'figure':

        a.communicate()
        df = filepath.split('.')[0]

    return df

# The below two functions help in searching for a word (pattern) within the text of a script if it is between single OR double quotes


def get_quoted_array(text):
    matches1 = re.findall(r'\"(.+?)\"', text)
    matches2 = re.findall(r"\'(.+?)\'", text)
    if len(matches1) > 0 and len(matches2) > 0:
        matches1.extend(matches2)
        return matches1
    elif len(matches1) > 0 and len(matches2) == 0:
        return matches1
    elif len(matches2) > 0 and len(matches1) == 0:
        return matches2
    else:
        return None


def find_substring_within_quotes(pattern, string):
    stringlist = get_quoted_array(string)
    stringlist = [x.lower() for x in stringlist]
    result = [word for word in stringlist if pattern.lower() in word]
    if len(result) > 0:
        return True
    else:
        return False
