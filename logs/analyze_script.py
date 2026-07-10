#!/usr/bin/env python
# coding: utf-8

# In[37]:


# import required modules
import numpy as np
import pandas as pd
from collections import Counter
import statistics

import random
import datetime
from datetime import date
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import rc
from datetime import datetime, date


# In[38]:


# import table from create_metadata_table.ipynb
allFactors = pd.read_csv("../data/metadata_matrix_raw.csv", low_memory=False)


# In[39]:


# add a column with the total use count of each converted_accession ID

# first, create a new dataframe with no double-counted accessions
# justAcc includes all unique pairings of PMC ID and Series/Study accession
justAcc = allFactors.loc[:, ["pmc_ID", "converted_accession"]]
justAcc = justAcc.drop_duplicates()

rc = Counter(justAcc["converted_accession"])
reuse_counts = pd.DataFrame.from_dict(rc, orient="index").reset_index()
reuse_counts.columns = ["converted_accession", "total_use_count"]
# reuse_counts


# In[40]:


# merge reuse counts onto table
allFactors = pd.merge(allFactors, reuse_counts, how="left", on="converted_accession")
# allFactors


# In[ ]:


# Convert 'repository_date' column to datetime objects
allFactors["repository_date"] = pd.to_datetime(
    allFactors["repository_date"], format="mixed"
)

# Fixed reference date for reproducibility — changing this would shift all norm_reuse_count values
REFERENCE_DATE = datetime(2026, 1, 1)

# Calculate the timedelta representing the amount of time public in years
allFactors["time_public"] = (REFERENCE_DATE - allFactors["repository_date"]).dt.days / 365

# Clip to minimum of 1 day: prevents division by zero and negative values for datasets
# deposited after the reference date (future deposits relative to 2026-01-01)
allFactors["time_public"] = allFactors["time_public"].clip(lower=1/365)

# Normalize 'reuse_count' by the amount of time public
allFactors["reuse_count"] = allFactors["total_use_count"] - 1
allFactors["norm_reuse_count"] = allFactors["reuse_count"] / allFactors["time_public"]

# Drop the 'total_use_count' column
allFactors.drop(columns=["total_use_count"], inplace=True)


# In[42]:


# Convert 'pmc_date' column to datetime format with the correct format string
allFactors["pmc_date"] = pd.to_datetime(allFactors["Date"], format="mixed", errors="coerce")


# In[ ]:


# Filter out rows with clearly corrupt future pmc_dates (e.g. 2028)
today = pd.Timestamp.today()
future_mask = allFactors['pmc_date'] > today
if future_mask.sum() > 0:
    print(f'Dropping {future_mask.sum()} rows with future pmc_date (>{today.date()})')
    allFactors = allFactors[~future_mask].copy()


# In[43]:


# Tag each paper as either a generator or a reuser

# take each converted accession along with the first date it appears in a paper
justPap = allFactors.loc[:, ["converted_accession", "pmc_date"]].drop_duplicates()
justPap = justPap.groupby("converted_accession", as_index=False).agg(
    {"pmc_date": "min"}
)
justPap = justPap.rename(columns={"pmc_date": "min_pmc_date"})

# label all of the usages of these datasets on these dates with a 'G' (generators)
justPap.loc[:, "reuse_role"] = "G"
# justPap


# In[44]:


# merge these back onto the original table, matching dates back to papers
allFactors = pd.merge(
    allFactors,
    justPap,
    how="left",
    left_on=["converted_accession", "pmc_date"],
    right_on=["converted_accession", "min_pmc_date"],
)

# label every usage of a dataset that isn't 'G' (generator) as 'R' (reuser)
allFactors["allR"] = "R"
allFactors["reuse_role"] = allFactors["reuse_role"].fillna(allFactors["allR"])

# drop redundant columns
allFactors = allFactors.drop(labels=["min_pmc_date", "allR"], axis=1)

# allFactors


# In[45]:


# perform QC on generator/reuser tags...

# cut out whitespace
allFactors.loc[:, "pmc_date"] = allFactors["pmc_date"].replace({" ": ""})

# take slice of what we need from allFactors — use .copy() to avoid SettingWithCopyWarning
gens = allFactors.loc[allFactors["reuse_role"] == "G", :].copy()

# convert to date objects
gens["pmc_date"] = pd.to_datetime(gens["pmc_date"], format="mixed")
gens["repository_date"] = pd.to_datetime(gens["repository_date"], format="mixed")

# subtract columns of date objects, creating a column of timedelta objects
gens["pub_delay"] = gens["pmc_date"] - gens["repository_date"]
gens = gens.dropna(subset=["pub_delay"])

# convert timedeltas back to integers
gens["pub_delay"] = gens["pub_delay"].dt.days
gens = gens.sort_values(by="pub_delay", ascending=False)

gens


# In[ ]:


# plot pub_delay distribution
fig_dims = (18, 12)
fig, ax = plt.subplots(figsize=fig_dims)
sns.histplot(gens["pub_delay"], ax=ax)


# In[47]:


# select only papers in a reasonable timedelta window

DELAY_MIN = -270
DELAY_MAX = 270

gens_filtered = gens.loc[gens["pub_delay"] <= DELAY_MAX, :].loc[
    gens["pub_delay"] >= DELAY_MIN, :
]
gens_filtered


# In[48]:


# select random papers inside/outside threshold for QC analysis

# gens_filtered are inside threshold already
# get papers outside threshold, but within 365 days

DELAY_EDGE = 365

gens_aboveThresh = gens.loc[gens["pub_delay"] >= DELAY_MAX, :].loc[
    gens["pub_delay"] <= DELAY_EDGE
]
gens_aboveThresh = gens_aboveThresh.loc[gens_aboveThresh["repository"] == "GEO", :]
gens_belowThresh = gens.loc[gens["pub_delay"] <= DELAY_MIN, :].loc[
    gens["pub_delay"] >= -DELAY_EDGE
]
gens_belowThresh = gens_belowThresh.loc[gens_belowThresh["repository"] == "GEO", :]
gens_insideThresh = gens_filtered.loc[gens_filtered["repository"] == "GEO", :]

num_papers = 50
half_papers = int(num_papers / 2)

print(f"inside: {len(gens_insideThresh['pmc_ID'].unique())}, "
      f"above: {len(gens_aboveThresh['pmc_ID'].unique())}, "
      f"below: {len(gens_belowThresh['pmc_ID'].unique())}")

# take random samples — capped at available population size
n_in = min(num_papers,   len(gens_insideThresh["pmc_ID"].unique()))
n_ab = min(half_papers,  len(gens_aboveThresh["pmc_ID"].unique()))
n_be = min(half_papers,  len(gens_belowThresh["pmc_ID"].unique()))

rand_in = pd.DataFrame(random.sample(gens_insideThresh["pmc_ID"].tolist(), n_in))
rand_in.columns = ["pmc_ID"]
rand_ab = pd.DataFrame(random.sample(gens_aboveThresh["pmc_ID"].tolist(), n_ab))
rand_ab.columns = ["pmc_ID"]
rand_be = pd.DataFrame(random.sample(gens_belowThresh["pmc_ID"].tolist(), n_be))
rand_be.columns = ["pmc_ID"]

# match accessions to papers to make manual work easier
r_in = pd.merge(
    rand_in, gens_insideThresh[["pmc_ID", "accession"]], on="pmc_ID", how="left"
).drop_duplicates()
r_in["real_introducer"] = np.nan
r_ab = pd.merge(
    rand_ab, gens_aboveThresh[["pmc_ID", "accession"]], on="pmc_ID", how="left"
).drop_duplicates()
r_be = pd.merge(
    rand_be, gens_belowThresh[["pmc_ID", "accession"]], on="pmc_ID", how="left"
).drop_duplicates()
r_out = pd.concat([r_ab, r_be])
r_out["real_introducer"] = np.nan

# save incomplete files to csv
r_in.to_csv("../data/random_introducers_inside.csv", index=False)
r_out.to_csv("../data/random_introducers_outside.csv", index=False)


# In[ ]:


not_gens = allFactors.loc[allFactors['reuse_role'] != 'G', :].copy()

# Compute pub_delay for reusers and filter out those with negative delay
# (negative = paper accepted before dataset deposit date = can't be a real reuser)
not_gens['pmc_date_dt'] = pd.to_datetime(not_gens['pmc_date'], format='mixed', errors='coerce')
not_gens['repo_date_dt'] = pd.to_datetime(not_gens['repository_date'], format='mixed', errors='coerce')
not_gens['pub_delay_days'] = (not_gens['pmc_date_dt'] - not_gens['repo_date_dt']).dt.days
neg_mask = not_gens['pub_delay_days'] < 0
print(f'Dropping {neg_mask.sum():,} Reuser rows with negative pub_delay')
not_gens = not_gens[~neg_mask].drop(columns=['pmc_date_dt', 'repo_date_dt', 'pub_delay_days'])

filtered_matrix = pd.concat(objs = [gens_filtered, not_gens], ignore_index = True, sort = False)


# In[ ]:


# Correct reuse_count: COUNT(DISTINCT pmc_ID WHERE reuse_role='R') per accession
# Paper methodology: generators get the count of unique papers that reused their dataset
reuser_counts = (
    filtered_matrix[filtered_matrix['reuse_role'] == 'R']
    .groupby('converted_accession')['pmc_ID']
    .nunique()
    .rename('reuse_count')
)
filtered_matrix['reuse_count'] = (
    filtered_matrix['converted_accession']
    .map(reuser_counts)
    .fillna(0)
    .astype(int)
)
filtered_matrix['norm_reuse_count'] = (
    filtered_matrix['reuse_count'] / filtered_matrix['time_public']
)


# In[ ]:


lsc = Counter(filtered_matrix.loc[filtered_matrix['repository'] == "SRA", ]["library_strategy"])
lsc_df = pd.DataFrame.from_dict(lsc, orient = 'index').reset_index()
sraLS = list(lsc_df["index"])
sraLS.remove("RNA-Seq")


# In[ ]:


# add a column for "class"... separating GEO/SRA distinction into:
# (1) MGED  - GEO gene expression microarray
# (2) RSGED - RNA-Seq deposited on GEO
# (3) RSRA  - RNA-Seq directly in SRA
# (4) WGSRA - other SRA omics (WGS, ChIP-seq, ATAC-seq, etc.)

key = {
      "repository": ["GEO", "GEO", "SRA"] + ["SRA"] * (len(sraLS)),
      "library_strategy": ["Expression_Array", "RNA-Seq", "RNA-Seq"] + sraLS,
      "class": ["MGED", "RSGED", "RSRA"] + ["WGSRA"] * (len(sraLS))}
key = pd.DataFrame(key)
key


# In[ ]:


filtered_matrix = pd.merge(filtered_matrix, key, how = 'left', on = ['repository', 'library_strategy'])
filtered_matrix


# In[ ]:


# Fix null class for GEO rows: use geo_series 'Series Type' to classify
# These are GSE accessions with no SRA metadata (library_strategy is null)
geo_types = pd.read_csv('../data/geo_series.csv', low_memory=False,
                         usecols=['Accession', 'Series Type'])
geo_types.columns = ['converted_accession', 'series_type']

def series_type_to_class(s):
    if not isinstance(s, str):
        return np.nan
    s = s.lower()
    if 'high throughput sequencing' in s or 'high-throughput sequencing' in s:
        return 'RSGED'
    if ('array' in s or 'chip' in s or 'spotted' in s or
        'nlaiii' in s or 'mpss' in s or 'rt-pcr' in s):
        return 'MGED'
    return np.nan

geo_types['class_fallback'] = geo_types['series_type'].apply(series_type_to_class)

null_mask = filtered_matrix['class'].isnull() & (filtered_matrix['repository'] == 'GEO')
filtered_matrix = filtered_matrix.merge(
    geo_types[['converted_accession', 'class_fallback']],
    on='converted_accession', how='left'
)
filtered_matrix.loc[null_mask, 'class'] = (
    filtered_matrix.loc[null_mask, 'class_fallback']
)
filtered_matrix.drop(columns=['class_fallback'], inplace=True)

recovered = null_mask.sum() - filtered_matrix['class'].isnull().sum()
print(f'Recovered {recovered:,} null-class GEO rows via Series Type')
print(f'Still null: {filtered_matrix["class"].isnull().sum():,}')


# In[ ]:


# what are the SRA library strategies?
lc = Counter(filtered_matrix.loc[filtered_matrix["repository"] == "SRA", :]["library_strategy"])
libCntr = pd.DataFrame.from_dict(lc, orient = "index")
libCntr.to_csv("../data/sra_library_strategies.csv", header = False)


# In[ ]:


# how much data coverage do we get when using the class column?
Counter(filtered_matrix['class'])


# In[ ]:


filtered_matrix.to_csv('../data/metadata_matrix_filtered.csv', index = False)


# In[ ]:


# generate papers.csv: one row per unique paper with year column
# this is used by visualize.ipynb
papers = filtered_matrix[['pmc_ID', 'repository', 'reuse_role', 'pmc_date', 'library_strategy', 'class']].copy()
papers['pmc_date'] = pd.to_datetime(papers['pmc_date'], errors='coerce')
papers['year'] = papers['pmc_date'].dt.year
papers = papers.drop_duplicates(subset=['pmc_ID', 'repository', 'reuse_role', 'class'])
papers.to_csv('../data/papers.csv', index=False)
papers


# In[ ]:




