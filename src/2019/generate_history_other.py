# Repurposed from https://gist.github.com/jmlrt/f524e1a45205a0b9f169eb713a223330

from __future__ import (unicode_literals, absolute_import, print_function,
                        division)
from dotenv import load_dotenv, find_dotenv
import os
import sys
from sys import exit
import ast

# Import Google libraries
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pydrive.files import GoogleDriveFileList
import googleapiclient.errors
import time
import psycopg2

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")

sys.path.insert(0, GITHUB + "/General_Resources/common_functions/")
from postgres_helpers import *


def authenticate():
    """
        Authenticate to Google API
    """

    gauth = GoogleAuth()

    return GoogleDrive(gauth)


def get_folder_id(drive, parent_folder_id, folder_name):
    """
        Check if destination folder exists and return it's ID
    """

    # Auto-iterate through all files in the parent folder.
    file_list = GoogleDriveFileList()
    try:
        file_list = drive.ListFile(
            {'q': "'{0}' in parents and trashed=false".format(parent_folder_id)}
        ).GetList()
    # Exit if the parent folder doesn't exist
    except googleapiclient.errors.HttpError as err:
        # Parse error message
        message = ast.literal_eval(err.content)['error']['message']
        if message == 'File not found: ':
            print('  ' + message + folder_name)
            exit(1)
        # Exit with stacktrace in case of other error
        else:
            raise

    # Find the the destination folder in the parent folder's files
    for file1 in file_list:
        if file1['title'] == folder_name:
            return file1['id']


def create_folder(drive, folder_name, parent_folder_id):
    """
        Create folder on Google Drive
    """

    folder_metadata = {
        'title': folder_name,
        # Define the file type as folder
        'mimeType': 'application/vnd.google-apps.folder',
        # ID of the parent folder
        'parents': [{"kind": "drive#fileLink", "id": parent_folder_id}]
    }

    folder = drive.CreateFile(folder_metadata)
    folder.Upload()

    # Return folder informations
    print('  created: %s' % (folder['title']))
    print('id: %s' % (folder['id']))
    return folder['id']


def upload_files(drive, folder_id, filename, type_of_output):
    """
        Upload files in the local folder to Google Drive
    """
    if type_of_output == 'table':
        ext = '.csv'
        outpath = 'data/'
    else:
        ext = '.png'
        outpath = 'figure_images/'

    # Enter the source folder
    os.chdir(GITHUB + '/Projects/sots-isl/' + outpath)
    # make sure all files are executable
    os.system("chmod +x *")

    if os.path.exists(filename):
        print('  uploading ' + filename)
        # Upload file to folder.
        f = drive.CreateFile(
            {"parents": [{"kind": "drive#fileLink", "id": folder_id}]})
        f.SetContentFile(filename)
        new_title = filename.split('.')[0] + '_' + time.strftime("%Y_%m_%d_%H%M") + ext
        print('  ' + new_title)
        f['title'] = new_title
        f.Upload()
    else:
        print('  file {0} is does not exist'.format(filename))

    return new_title


def upload_hist(filename, type_of_output, parent_folder_id):

    drive = authenticate()
    if type_of_output == 'table':
        ext = '.csv'
    else:
        ext = '.png'
    # Get destination folder ID, or create if it doesn't exist
    folder_id = get_folder_id(drive, parent_folder_id, filename)
    if folder_id is None:
        folder_id = create_folder(drive, filename, parent_folder_id)

    # Upload the figure with today's timestamp
    new_title = upload_files(drive, folder_id, filename + ext, type_of_output)

    # # get link of new file for output
    file_list = GoogleDriveFileList()
    file_list = drive.ListFile(
        {'q': "'{0}' in parents and trashed=false".format(folder_id)}).GetList()

    for file1 in file_list:
        if file1['title'] == new_title:
            link = file1['embedLink']

    return folder_id, link


def insert_table_db(connection, cursor, df, tablename):
    """
        Inserts a dataframe into a Postgres table with ``tablename``. Must pass in the ``psycopg2`` connection and cursor objects.
    """

    cursor.execute("delete from dl." + tablename + " where create_date::date = current_date;")
    insert_large_table(df, 'dl', tablename, cursor)


def insert_table_hist(df, df_exists, tablename, HOST, USER, PASSWORD, DB):
    """
        Uses the above 3 functions to insert a dataframe into a history table with the name ``tablename``, even if the table does not yet exist.
        Must pass in the Postgres connection paramaters.
    """

    myConnection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, database=DB)
    cursor = myConnection.cursor()
    print(str(df_exists['exists'].item()))
    if str(df_exists['exists'].item()) == 'False':
        # create the table if it doesnt exist
        headers, type_list = generate_headers(df)
        headers.insert(0, 'id')
        type_list.insert(0, 'serial')
        statement = create_table_sql('dl', tablename, headers, type_list, ['id'])
        cursor.execute("rollback")
        cursor.execute(statement)

    insert_table_db(myConnection, cursor, df, tablename)
