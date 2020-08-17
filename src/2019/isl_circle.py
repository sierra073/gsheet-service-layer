# README
# To save log output to a .txt: python isl_main.py > DDMonthYYYY_log.txt
# To resolve OSError, directory not found, try editing your .bash_profile and/or .env files:
#   nano /Users/yourusername/.bash_profile
#   nano /Users/yourusername/.env

# load libraries
import sys
import os
import pandas as pd
import warnings
import datetime
import re

# load custom python scripts
from read_from_google_sheet import get_master_as_dict
from execution_modules import run_sql, run_script
from generate_output import generate_output, output_change_to_description
from generate_history_value import insert_value_db

# settings
warnings.filterwarnings("ignore")


# get the GitHub (ficher) path base
global GITHUB
# load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")

# add the analysis scripts path to the system path
# have to ensure that your path variables don't end with "/"
if GITHUB.endswith("/"):
    try:
        sys.path.insert(0, GITHUB + 'Projects/sots-isl/scripts/2019')
    except OSError as ose:
        print "The file can't be found. Check that the .env file is in the location the script is searching. The script looks in the location returned by the Python command: 'find_dotenv().' If the location searched is correct, then check the .env file to ensure the PATH variable, GITHUB, is correctly defined."
else:
    try:
        os.chdir(GITHUB + '/Projects/sots-isl/scripts/2019')
    except OSError as ose:
        print "The file can't be found. Check that the .env file is in the location the script is searching. The script looks in the location returned by the Python command: 'find_dotenv().' If the location searched is correct, then check the .env file to ensure the PATH variable, GITHUB, is correctly defined."

# define database credentials -- used for all modules
global HOST_DAR, HOST_DAR_HIST, USER_DAR, USER_DAR_HIST, PASSWORD_DAR, PASSWORD_DAR_HIST, DB_DAR, DB_DAR_HIST
HOST_DAR_HIST = "ec2-18-205-200-121.compute-1.amazonaws.com"
USER_DAR_HIST = os.environ.get("USER_DAR_HIST")
PASSWORD_DAR_HIST = os.environ.get("PASSWORD_DAR_HIST")
DB_DAR_HIST = os.environ.get("DB_DAR_HIST")

HOST_DAR = os.environ.get("HOST_DAR")
USER_DAR = os.environ.get("USER_DAR")
PASSWORD_DAR = os.environ.get("PASSWORD_DAR")
DB_DAR = os.environ.get("DB_DAR")

# get relevant ids from current branch
ids = []
for line in open(GITHUB + '/Projects/sots-isl/changed_files.txt', 'r'):
    if re.search(r'id\d', line) is not None:
        ids.append(line.rstrip())
try:
    ids = [f.split('/')[4].split('_')[0][2:] for f in ids]
except Exception:
    pass
print('changed ids: ', ids)

# pull in the first sheet of Master Metric Tracker (MMT) in dictionary form
mm_sheet, mm_dict = get_master_as_dict("Master Metric Tracker - SotS 2019")

# for every record (question/insight) pulled from Master Metric Tracker (MMT), execute the script and populate the output in the output column cell.
start_time = datetime.datetime.now()
print('Script start time: ' + str(start_time) + '\n')

rownum = 1
error_count = 0
for rec in mm_dict:
    rownum += 1

    if (rec['Request Group'] != "2018 SOTS") and (str(rec['input_id']) in ids):
        # error handling

        input_id = rec['input_id']

        if not rec['input_id']:
            print(str(input_id) + "  Must provide an input id. This error may be due to a blank row in the Master Metric Tracker (MMT)")
            continue

        if rec['Metric/Insight']:
            insight = rec['Metric/Insight']
        else:
            print(str(input_id) + ": Must provide a Metric/Insight")
            continue

        if rec['input_filepath']:
            script = rec['input_filepath']
        else:
            print(str(input_id) + ": Must provide a script (input_filepath)")
            continue
        scriptname = script.split('.')[0]

        # diagnostic log information
        print('\n' + '-' * min(max(len(insight) + 16, len(script)), 80))
        print('input_id: ' + str(input_id))
        print('metric/insight: ' + str(insight.encode("utf-8")))    #
        print('input script: ' + script)
        print('-' * min(max(len(insight) + 16, len(script)), 80))
        print('\n' + '******************************')

        if rec['input_format_of_output'] and (rec['input_format_of_output'].lower() == 'value' or rec['input_format_of_output'].lower() == 'table' or rec['input_format_of_output'].lower() == 'figure'):
            type_of_output = rec['input_format_of_output'].lower()
        else:
            print(str(input_id) + ": Must provide a valid format of output (value, table, figure)")
            continue

        if script.split('.')[1].lower() == 'sql':
            software = 'sql'
            if rec['input_software'].lower() != 'sql':
                print(str(input_id) + ": please check input_software, attempting to run sql")
        elif script.split('.')[1].lower() == 'r':
            software = 'R'
            if rec['input_software'].lower() != 'r':
                print(str(input_id) + ": please check input_software, attempting to run R")
        elif script.split('.')[1].lower() == 'py':
            software = 'python'
            if rec['input_software'].lower() != 'python':
                print(str(input_id) + ": please check input_software, attempting to run python")
        else:
            software = 'tableau'

        # if there's a change in description, notify owner
        output_change_to_description(mm_sheet, rownum, insight, input_id, HOST_DAR_HIST, USER_DAR_HIST, PASSWORD_DAR_HIST, DB_DAR_HIST)
        # output description history
        print("outputting description history")
        desc_df = pd.DataFrame({'input_id': input_id, 'description': insight}, index=[0])
        insert_value_db(desc_df, 'desc', HOST_DAR_HIST, USER_DAR_HIST, PASSWORD_DAR_HIST, DB_DAR_HIST)

        # run scripts and generate output for each script type
        if software == 'sql':
            try:
                output_df = run_sql(script, HOST_DAR, USER_DAR, PASSWORD_DAR, DB_DAR)

                if type_of_output == 'table':
                    try:
                        os.remove(GITHUB + '/Projects/sots-isl/data/' + scriptname + '.csv')
                    except OSError:
                        pass
                    output_df.to_csv(GITHUB + '/Projects/sots-isl/data/' + scriptname + '.csv')

                generate_output(insight, input_id, scriptname, type_of_output, output_df, rownum)
            except Exception as e:
                print('ERROR :( check script meets ISL requirements')
                error_count += 1
                print(e)
                continue

        elif software == 'python' or software == 'R':
            try:
                output_df = run_script(script, software, type_of_output)
                generate_output(insight, input_id, scriptname, type_of_output, output_df, rownum)
            except Exception as e:
                print('ERROR :( check script meets ISL requirements')
                error_count += 1
                print(e)
                continue

print('******************************' + '\n' + '\n' + '\n' + '*' * 14 + '\n' + '* END OF LOG *' + '\n' + '*' * 14 + '\n')
end_time = datetime.datetime.now()
duration = end_time - start_time
duration_in_s = duration.total_seconds()      # Total number of seconds between dates
minutes = divmod(duration_in_s, 60)[0]        # Duration in minutes
print('Script start time: ' + str(start_time) + '\n')
print('Script end time: ' + str(end_time))
print('Total run time (minutes): ' + str(minutes))
print('Total run time (seconds): ' + str(duration_in_s))

if error_count > 0:
    raise Exception('there were ' + str(error_count) + ' errors!')
