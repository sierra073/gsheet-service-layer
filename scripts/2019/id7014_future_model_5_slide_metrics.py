from __future__ import division
import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
GITHUB = os.environ.get("GITHUB")

districts = pd.read_csv(GITHUB+'''data/id7011_future_model_5_df.csv')
students = pd.read_csv(GITHUB+'''data/id7013_future_model_5_students_df.csv')

districts.set_index('actual_year',inplace=True)
students.set_index('actual_year',inplace=True)

def agg_metrics(df):
    meeting_overall = round((df.loc['Total']['Peer Combo'] +  df.loc['Total']['Already Meeting'])/df.loc['Total']['row_total'],2)
    not_meeting = round(df.loc['Total']['Peer Combo']/( df.loc['Total']['row_total']-df.loc['Total']['Already Meeting']),2)
    summary_df = pd.DataFrame(data={'out_of_meeting_overall':[meeting_overall],'out_of_not_currently_meeting':[not_meeting]})
    return summary_df

students_summ = agg_metrics(students)
districts_summ = agg_metrics(districts)

students_summ['units'] = 'students'
districts_summ['units'] = 'districts'

final_df = pd.concat([districts_summ,students_summ])
final_df = final_df[['units','out_of_not_currently_meeting','out_of_meeting_overall']]

final_df.to_csv(GITHUB+'/''data/'+os.path.basename(__file__).replace('.py','.csv'),index=False)