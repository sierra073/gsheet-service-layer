import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

GITHUB = os.environ.get("GITHUB")

best_deal = pd.read_csv(GITHUB+'/''data/id7024_best_deal_districts.csv')

best_deal['total_bw_percent_change'] = (best_deal.total_bw_percent_change*100).round()

# indicate bins for categories
max_bin = 320
increment = 10
bins = np.arange(0, max_bin, increment)

# create labels
labels = []
for i, b in enumerate(bins):
    if i == 0:
        labels.append("less than " + str(bins[i+1]) + '%')
    elif i < len(bins)-1:
        labels.append(str(bins[i])+'%')
    else:
        break
# add final category to labels
labels.append(str(max_bin-increment) +'% or over')

# categorize bw_pct_change
best_deal.loc[:,'bw_pct_change_category'] = pd.cut(best_deal['total_bw_percent_change'],
                                                     bins=bins,
                                                     labels=labels[:-1], right=False)

# add new category
best_deal.loc[:, 'bw_pct_change_category'] = best_deal['bw_pct_change_category'].cat.add_categories(str(max_bin-increment) + '% or over')

# fill in Nans (anything over max_bin)
best_deal[['bw_pct_change_category']] = best_deal[['bw_pct_change_category']].fillna(value=str(max_bin-increment) +'% or over')

# change index to string, and count values for plotting
best_deal.loc[:, 'bw_pct_change_category'] = best_deal['bw_pct_change_category'].astype(str)

# converting list to df for merging
df_temp = pd.DataFrame({'labels': labels})
df_temp.set_index('labels', inplace=True)

# count instances of category for district count
df_pct_change = best_deal.bw_pct_change_category.value_counts().to_frame()

# merge into resultant dataframe to keep order of index
df_pct_change = df_temp.merge(df_pct_change, left_index=True, right_index=True )
df_pct_change.reset_index(inplace=True)

# rename index column
df_pct_change.columns = ['pct_category', 'district_count']

## add running total column
df_pct_change['district_running_total'] = df_pct_change.district_count.cumsum()

df_pct_change['district_running_total_percent_total'] = df_pct_change.district_running_total/df_pct_change.district_count.sum()

# plotting the figure
plt.figure(figsize=(20, 7))
xmarks = np.arange(0, df_pct_change.shape[0])
plt.bar(xmarks, df_pct_change['district_count'], color='#009296')

# ticks, titles, axis labels
plt.ylabel("Districts")
plt.xlabel("Bandwidth Increase with Best Deal")
plt.xticks(xmarks, df_pct_change.pct_category.values, rotation=20)
plt.box(on=None)
plt.yticks([])
for x0,v0, label in zip (xmarks,df_pct_change['district_count'],
                         df_pct_change['district_count']):
    plt.text(x0-.25,v0+10,label)

plt.savefig(GITHUB+'/''figure_images/'+os.path.basename(__file__).replace('.py','.png'))