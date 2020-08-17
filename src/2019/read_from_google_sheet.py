import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv, find_dotenv

# get the GitHub (ficher) path base
global GITHUB
load_dotenv(find_dotenv()) # find_dotenv() returns the directory of your .env file
GITHUB = os.environ.get("GITHUB"); GITHUB


def gspread_connect():
    """Authorizes ``gspread`` for a given set of credentials (stored in creds.json in the same directory).
    Returns the set of files that the given user can connect to using the API.
    """
    # change directory
    os.chdir(GITHUB + '/Projects/sots-isl')

    # connect to the Google Sheet
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)  # get email and key from creds

    files = gspread.authorize(credentials)  # authenticate with Google

    return files


def get_master(gsheet_filename):
    """Uses ``gspread`` to pull the Master Master Google Sheet as an object.
    """
    # general function to start using ``gspread``
    files = gspread_connect()

    # get entire Google Sheet ("Spreadsheet" class)
    master_master = files.open(gsheet_filename)

    return master_master


def get_master_as_dict(gsheet_filename):
    """Uses ``gspread`` to pull from the Master Master Google Sheet and returns:
    * the ``gspread`` 'Worksheet object' form of the first worksheet (``mm_sheet``)
    * the first worksheet as a list of dictionaries (``mm_dict``): each record (row) is a dictionary where the keys represent the columns
    (``gspread`` detects the first row as the header with the column names that are used throughout), and the values represent the value in the correspinding cell for each column.
    """

    # get entire Google Sheet ("Spreadsheet" class) + first sheet ("Worksheet" class)
    master_master = get_master(gsheet_filename)
    mm_sheet = master_master.sheet1

    mm_dict = mm_sheet.get_all_records()

    return mm_sheet, mm_dict
