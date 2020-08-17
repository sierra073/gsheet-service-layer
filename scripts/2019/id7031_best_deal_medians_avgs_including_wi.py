import os
import pandas as pd
import numpy as np

GITHUB = os.environ.get("GITHUB")

best_deal = pd.read_csv(GITHUB+'/Projects/sots-isl/data/id7030_best_deal_districts_including_wi.csv')
best_deal = best_deal[best_deal.funding_year == 2019]
best_deal['total_bw_percent_change'] = best_deal['total_bw_percent_change']*100

def calc_summary_metrics(df):
    medians = pd.DataFrame({'metric':['median'],
    'bw_percent_increase':[df.total_bw_percent_change.median()],
    'bw_increase_mbps':[df.total_bw_change.median()],
    'bw_before_deal_mbps':[df.old_total_bw.median()],
    'bw_after_deal_mbps':[df.new_total_bw.median()],
    'bw_per_student_before_deal_kbps':[df.old_bw_per_student.median()],
    'bw_per_student_after_deal_kbps':[df.new_bw_per_student.median()]})

    averages = pd.DataFrame({'metric':['mean'],
    'bw_percent_increase':[df.total_bw_percent_change.mean()],
    'bw_increase_mbps':[df.total_bw_change.mean()],
    'bw_before_deal_mbps':[df.old_total_bw.mean()],
    'bw_after_deal_mbps':[df.new_total_bw.mean()],
    'bw_per_student_before_deal_kbps':[df.old_bw_per_student.mean()],
    'bw_per_student_after_deal_kbps':[df.new_bw_per_student.mean()]})

    df_metrics = pd.concat([medians,averages])
    df_metrics = df_metrics[['metric','bw_percent_increase','bw_increase_mbps','bw_before_deal_mbps','bw_after_deal_mbps','bw_per_student_before_deal_kbps','bw_per_student_after_deal_kbps']]
    return df_metrics

best_deal_medians_avgs = calc_summary_metrics(best_deal).round()

best_deal_medians_avgs.to_csv(GITHUB+'/Projects/sots-isl/data/'+os.path.basename(__file__).replace('.py','.csv'),index=False)