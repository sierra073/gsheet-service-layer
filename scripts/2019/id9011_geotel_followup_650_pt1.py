# 1) What percent of peer deals (old meth) are from these 650 providers?
# 2) How many districts with peer deals have peer deals ONLY from these providers (out of all districts with peer deals)?

from __future__ import division
import os
import pandas as pd
pd.options.display.max_columns = 999

GITHUB = os.environ.get("GITHUB")
DATA_PATH = GITHUB + '/Projects/peer_deal_methodology_updates_testing/data/'

# read in data
peer_deals_old_meth_sent_geotel = pd.read_csv(DATA_PATH + 'raw/districts_peersps_071819.csv')
esh_sp_parent_names = pd.read_csv(DATA_PATH + 'processed/esh_sps_list.csv')  # 728 provider parent names
esh_sp_parent_names['esh_sp'] = esh_sp_parent_names['esh_sp'].apply(lambda s: s.upper())
esh_650_not_matched = pd.read_csv(DATA_PATH + 'processed/esh_sps_not_80_geotel_sps.csv')

# filter to districts and their peer sp parent names only (we provided them doing business as name AND parent name, i.e. duplicated for each provider)
peer_deals_old_meth = peer_deals_old_meth_sent_geotel[['district_id', 'peersp']].drop_duplicates()
peer_deals_old_meth['peersp'] = peer_deals_old_meth['peersp'].fillna('').apply(lambda s: s.upper())
peer_deals_old_meth = peer_deals_old_meth[peer_deals_old_meth.peersp.isin(esh_sp_parent_names['esh_sp'])]
# pull in the 650 providers
peer_deals_old_meth = peer_deals_old_meth.merge(esh_650_not_matched, how='left', left_on='peersp', right_on='esh_sp')
peer_deals_old_meth = peer_deals_old_meth.rename(columns={'esh_sp': 'esh_650_sp'})

# 1)
peer_deals_old_meth_count_notna = peer_deals_old_meth.count()
pct_old_meth_deals_from_650 = peer_deals_old_meth_count_notna['esh_650_sp']/peer_deals_old_meth_count_notna['peersp']

# 2)
districts_deals_counts = peer_deals_old_meth.groupby('district_id')['peersp'].count().reset_index().rename(columns={'peersp': 'num_deals'})
districts_650_deals_counts = peer_deals_old_meth[peer_deals_old_meth.peersp == peer_deals_old_meth.esh_650_sp].groupby('district_id')['peersp'].count().reset_index().rename(columns={'peersp': 'num_deals_650'})
peer_deals_old_meth = peer_deals_old_meth.merge(districts_deals_counts, how='inner', on='district_id')
peer_deals_old_meth = peer_deals_old_meth.merge(districts_650_deals_counts, how='left', on='district_id')
peer_deals_old_meth['num_deals_650'] = peer_deals_old_meth['num_deals_650'].fillna(0)

num_districts_deals_only_from_650 = peer_deals_old_meth[peer_deals_old_meth.num_deals == peer_deals_old_meth.num_deals_650].district_id.nunique()
all_districts_with_peer_deals = peer_deals_old_meth.district_id.nunique()


results = pd.DataFrame({
          'Pct of old deals from 650': pct_old_meth_deals_from_650,
          'Districts w/ deals ONLY from 650': num_districts_deals_only_from_650,
          'All districts with peer deals': all_districts_with_peer_deals
          }, index=[0])

# save csv
os.chdir(GITHUB + '/Projects/sots-isl/data')
results.to_csv("id9011_geotel_followup_650_pt1.csv", index=False)
