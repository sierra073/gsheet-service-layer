import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv, find_dotenv
import matplotlib.pyplot as plt

GITHUB = os.environ.get("GITHUB")
best_deal = pd.read_csv(GITHUB+'/''data/id7024_best_deal_districts.csv')

max_bin = 1100
increment = 100
bins = np.arange(0, max_bin, increment)

# create labels
labels = []
for i, b in enumerate(bins):
    if i == 0:
        labels.append("less than " + str(bins[i+1]))
    elif i < len(bins)-1:
        labels.append(str(bins[i]))
    else:
        break
# add final category to labels
labels.append(str(max_bin-increment)+ ' or over')

def categorize_column(column):
	# categorize bw_pct_change
	best_deal.loc[:,'category'] = pd.cut(best_deal[column],
	                                                     bins=bins,
	                                                     labels=labels[:-1], right=False)

	# add new category
	best_deal.loc[:, 'category'] = best_deal['category'].cat.add_categories(str(max_bin-increment)+ ' or over')

	# fill in Nans (anything over max_bin)
	best_deal[['category']] = best_deal[['category']].fillna(value=str(max_bin-increment)+ ' or over')

	# change index to string, and count values for plotting
	best_deal.loc[:, 'category'] = best_deal['category'].astype(str)

	# converting list to df for merging
	df_temp = pd.DataFrame({'labels': labels})
	df_temp.set_index('labels', inplace=True)

	# count instances of category for district count
	df = best_deal.category.value_counts().to_frame()

	# merge into resultant dataframe to keep order of index
	df = df_temp.merge(df, left_index=True, right_index=True )
	df.reset_index(inplace=True)

	return df 

new_bw = categorize_column('new_bw_per_student')
new_bw.columns = ['bw_per_student_category','district_count_after']

old_bw = categorize_column('old_bw_per_student')
old_bw.columns = ['bw_per_student_category','district_count_before']

final_df = pd.merge(new_bw,old_bw,on='bw_per_student_category',how='outer').fillna(0)
final_df[['district_count_after','district_count_before']] = final_df[['district_count_after','district_count_before']].astype(int)

# plotting the figure
plt.rcParams['font.family'] = 'Lato'
plt.rcParams['font.size'] = 12
fig, (ax1, ax2) = plt.subplots(2, figsize=(15, 10))
xmarks = np.arange(0, final_df.shape[0])
ax1.bar(xmarks, final_df['district_count_before'], color='#90c84d')
ax2.bar(xmarks, final_df['district_count_after'], color='#009296')

# ticks, titles, axis labels
ax1.set_ylabel('Districts')
ax2.set_ylabel('Districts')
ax1.set_frame_on(False)
ax2.set_frame_on(False)
ax1.set_yticks([])
ax2.set_yticks([])
ax1.set_xlabel('Before Best Deals - Bandwidth per Student (kbps)')
ax2.set_xlabel('After Best Deals - Bandwidth per Student (kbps)')
ax1.set_xticks(xmarks)
ax2.set_xticks(xmarks)
ax1.set_xticklabels(final_df.bw_per_student_category.values)
ax2.set_xticklabels(final_df.bw_per_student_category.values)

## data labels
for x0,v0, label in zip (xmarks,final_df['district_count_before'],
                         final_df['district_count_before']):
    ax1.text(x0-.15,v0+2,label)
for x0,v0, label in zip (xmarks,final_df['district_count_after'],
                         final_df['district_count_after']):
    ax2.text(x0-.15,v0+2,label)

plt.savefig(GITHUB+'/''figure_images/'+os.path.basename(__file__).replace('.py','.png'))