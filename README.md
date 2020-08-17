# Integrated Services Layer in Google Sheets
This tool executes all scripts used to generate metrics (including tables and charts) for the EducationSuperHighway State of the States (SotS) Report and displays the metrics in a Google Sheet tracker, with the ability to refresh at any time. The tool also tracks the history of all metrics each time it runs so we can see how they evolve over time as the production data becomes more clean/stable.

## For End Users
Please refer to [this wiki](https://educationsuperhighway.atlassian.net/wiki/spaces/SA/pages/671383731/Integrated+Services+Layer) for how to use the tool to generate a SotS metric.

## For Developers
* The tool is built entirely in python - you should be able to use python 2.7 or 3, but python 2.7 is a safer bet
* You must have Java 8 installed (check using the `java -version` command)
* You must have R installed

### Google API credentials
3 credentials and configuration files are needed in order to execute the scripts because we are using Google Sheets and Google Drive APIs. See the [Python Quickstart](https://developers.google.com/sheets/api/quickstart/python) for the Sheets API and the [PyDrive docs](https://pythonhosted.org/PyDrive/oauth.html#automatic-and-custom-authentication-with-settings-yaml) for the necessary auth setup.
### Environment requirements
Your `.bash_profile` and `.env` should be set up with ESH environment variables - the `GITHUB` path and `DAR` database credentials are required at a minimum.
### Usage
The [isl_main.py](https://github.com/sierra073/gsheet-service-layer/blob/master/src/2019/isl_main.py) script found in `src/2019/` runs the tool.
### Interpretation of logs
Each time you run `isl_main.py`, there will be a stream of output that you can pipe to a log file. It prints details around every script being run and the errors (if any) that occurred during each script.
