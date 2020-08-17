'''
2019 Sots National Analysis Draft v1

Five years ago, only 30 percent of America's K-12 public schools districts
met the FCC's 100 Kbps per student Internet access goal. (Future, p13)

'''
from __future__ import division
import os
import psycopg2
import pandas as pd


GITHUB = os.environ.get("GITHUB")

# number of students meeting 100 kbps goal in 2013
os.chdir(GITHUB + '/''scripts/2019/prework_queries')
df_2013 = pd.read_excel('2013_districts_2018sots.xlsx')

# number of districts meeting 100 kpbs in 2013
num_districts_meeting2014_in_2013 = df_2013[df_2013['100+ kbps/ student'] > 0]['Lookup'].nunique()

# number of districts total
num_districts_total = df_2013['Lookup'].nunique()

# percent of districts meeting 100k goal in 2013
print(num_districts_meeting2014_in_2013/num_districts_total)
