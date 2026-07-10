#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import required modules

import numpy as np
import pandas as pd
import statistics

from collections import Counter
from tqdm import tqdm


# In[2]:


# Import GEO reference data

# import table with accession, platform, and series
geoAPS = pd.read_csv('../data/geo_samples.csv')

# import table with datasets IDs
geoSeries = pd.read_csv('../data/geo_series.csv', low_memory = False)
geoSeries.rename(columns={'Accession':'Series'}, inplace = True)

# add datasets column by merging
allData = pd.merge(geoAPS, geoSeries, how = 'outer', on = 'Series')
geoReference = allData[['Series', 'Accession', 'Platform', 'Datasets']].drop_duplicates()


# In[3]:


sra_reference_path = '../data/sra_complete_runs.csv'
# Only read columns needed downstream to save memory and time
SRA_COLS = ['Run', 'SRAStudy', 'Experiment', 'BioProject', 'Submission', 'Sample',
            'ReleaseDate', 'Model', 'LibraryStrategy', 'ScientificName',
            'bases', 'avgLength', 'Consent']
chunks = []
for chunk in pd.read_csv(sra_reference_path, chunksize=500_000,
                          usecols=lambda c: c in SRA_COLS, low_memory=False):
    chunks.append(chunk)
result_df = pd.concat(chunks, ignore_index=True)
print(f"SRA runs loaded: {result_df.shape}")


# In[4]:


# SRA loading done in cell above


# In[5]:


result_df.shape


# In[ ]:


# (5041744, 47)


# In[6]:


sraReference = result_df


# In[7]:


# import data scraped from PubMed

pmcData = pd.read_csv('../data/pre_filter_matrix.csv')
pmcData


# In[8]:


pmcData.columns = ['journal', 'pmc_ID', 'accession']


# In[9]:


# Merge GEO accessions with reference data, convert to Series

# match series first
se = pd.merge(pmcData, geoReference['Series'].drop_duplicates(), how = 'left',
            left_on = 'accession', right_on = 'Series')
pmcData = se.rename(columns = {'Series': 'Series_result'})

# match each other style of GEO ID
# NOTE: 'Platform' (GPL) is intentionally excluded — one platform maps to thousands
# of series, causing false dataset attribution (e.g. all GPL570 -> GSE100014)
for col in ['Accession', 'Datasets']:
    pmcData = pd.merge(pmcData, geoReference[['Series', col]].drop_duplicates(subset = col), how = 'left', 
            left_on = 'accession', right_on = col)
    label = col + '_result'
    pmcData = pmcData.rename(columns = {'Series': label})

# combine all GEO series match columns into one aggregate GEO series column, clean up
pmcData['geoSeries'] = pmcData['Series_result'].fillna(pmcData['Accession_result']).fillna(pmcData['Datasets_result'])
pmcData = pmcData.drop(labels = ['Accession', 'Datasets',
                           'Series_result', 'Accession_result',
                           'Datasets_result'], axis = 1)
pmcData_geoMerged = pmcData
pmcData_geoMerged


# In[10]:


# merge SRA accessions with reference data, convert to Study

# match SRA Study IDs first
st = pd.merge(pmcData_geoMerged, sraReference['SRAStudy'].drop_duplicates(), how = 'left',
            left_on = 'accession', right_on = 'SRAStudy')
pmcData = st.rename(columns = {'SRAStudy': 'Study_result'})

# match every other style of SRA ID
for col in ['Run', 'Experiment', 'BioProject', 'Submission', 'Sample']:
    pmcData = pd.merge(pmcData, sraReference[['SRAStudy', col]].drop_duplicates(subset = [col]), how = 'left',
                      left_on = 'accession', right_on = col)
    label = col + '_result'
    pmcData = pmcData.rename(columns = {'SRAStudy': label})

# combine all SRA Study matches into one aggregate column, clean up
pmcData['sraStudy'] = pmcData['Study_result'].fillna(pmcData['Run_result']).fillna(pmcData['Experiment_result']).fillna(pmcData['BioProject_result']).fillna(pmcData['Submission_result']).fillna(pmcData['Sample_result'])
pmcData = pmcData.drop(labels = ['Run', 'Experiment', 'BioProject', 'Submission', 'Sample',
                                'Study_result', 'Run_result', 'Experiment_result', 'BioProject_result',
                                'Submission_result', 'Sample_result'], axis = 1)

pmcData_sraMerged = pmcData
pmcData_sraMerged.sample(25)


# In[11]:


# combine GEO Series hits and SRA study hits into one converted accession column
pmcData['converted_accession'] = pmcData_sraMerged['geoSeries'].fillna(pmcData_sraMerged['sraStudy'])
pmcData = pmcData.drop(labels = ['geoSeries', 'sraStudy'], axis = 1)
pmcData


# In[12]:


# clean out garbage converted_accession entries, and rows that didn't map to a converted_accession
w = []
for a in pmcData['converted_accession']:
    if type(a) == str:
        if a[0:3] != 'GSE' and a[1:3] != 'RP':
                w.append(a)
w.append(np.nan)

pmcData = pmcData[~pmcData.converted_accession.isin(w)]
pmcData


# In[13]:


# perform QC with gold standard from Penn group (Casey + Kurt)
gsPMC = pd.read_table("/home/nicolaedrabcinski/open-science-analytics/data/pubmed_mappings.tsv")
gsPMC.columns = ["SRA_accession_code", "GEO_accession_code", "pm_ID", "pmc_ID"]
gsPMC


# In[14]:


# QC step: de-duplicate datasets present in both SRA and GEO
gsPMC_acc = gsPMC[['SRA_accession_code', 'GEO_accession_code']].dropna()
gsPMC_acc


# In[15]:


ovAcc = pd.merge(pmcData, gsPMC_acc, how = "left", left_on = "converted_accession", right_on = "SRA_accession_code")
ovAcc


# In[16]:


# Count the number of SRA datasets also present in GEO
numDupSRA = len(ovAcc["SRA_accession_code"]) - ovAcc["SRA_accession_code"].isna().sum()
print("duplicated SRA datasets: " + str(numDupSRA))


# In[17]:


# convert SRA ID of duplicated datasets to GEO ID
ovAccNA = ovAcc.loc[ovAcc["SRA_accession_code"].isna(), :].copy()
ovAcc = ovAcc.loc[~ovAcc["SRA_accession_code"].isna(), :].copy()
ovAcc['converted_accession'] = ovAcc["GEO_accession_code"]
ovAccNA


# In[18]:


pmcData = pd.concat([ovAcc, ovAccNA], axis = 0) 
pmcData = pmcData[['journal', 'pmc_ID', 'accession', 'converted_accession']]
pmcData


# In[19]:


# QC step: validate our publication-dataset relationships with gold standard table
# merge my PMC ID-accession information with gold standard
# in this table: only those papers in my data AND gold standard
ovPMC = pd.merge(
    gsPMC[
        ['pmc_ID', 'SRA_accession_code', 'GEO_accession_code']
    ],
    pmcData[
        ['pmc_ID', 'accession', 'converted_accession']
    ], 
    on = "pmc_ID", 
    how = "inner")
ovPMC


# In[20]:


# check for matches between both accession and converted accession, SRA and GEO
ovPMC['match'] = (ovPMC['converted_accession'] == ovPMC['GEO_accession_code']).astype(int)
ovPMC['match'] += (ovPMC['converted_accession'] == ovPMC['SRA_accession_code']).astype(int)
ovPMC['match'] += (ovPMC['accession'] == ovPMC['GEO_accession_code']).astype(int)
ovPMC['match'] += (ovPMC['accession'] == ovPMC['SRA_accession_code']).astype(int)
ovPMC['match'] = ovPMC['match'] > 0
ovPMC


# In[21]:


# group by paper: if a paper had at least one successful match, we count it as a success
ovPMC_byPap = ovPMC[['pmc_ID', 'match']].groupby("pmc_ID", as_index = False)['match'].max()
ovPMC_byPap = pd.DataFrame(ovPMC_byPap)
# ovPMC_byPap

ovPMC_byPap.loc[~ovPMC_byPap['match'], :]


# In[22]:


# count up results and report percentage
totRelsSub = len(ovPMC_byPap['pmc_ID'])
print("Number of papers overlapping with gold standard: " + str(totRelsSub))
totMatching = ovPMC_byPap['match'].sum()
print("Number of such papers validated by gold standard: " + str(totMatching))
pctVal = totMatching / totRelsSub
print("Percent validated: " + str(pctVal))


# In[23]:


sraAttributes = sraReference


# In[24]:


# Convert SRA dates to a universal format
pd.set_option('display.max_columns', 50)
sraAttributes['ReleaseDate'] = sraAttributes['ReleaseDate'].str[0:10]


# In[29]:


# Define functions to convert GEO dates to a universal format
def strToMonth(m):
    if(m == 'Jan'):
        return '01'
    elif(m == 'Feb'):
        return '02'
    elif(m == 'Mar'):
        return '03'
    elif(m == 'Apr'):
        return '04'
    elif(m == 'May'):
        return '05'
    elif(m == 'Jun'):
        return '06'
    elif(m == 'Jul'):
        return '07'
    elif(m == 'Aug'):
        return '08'
    elif(m == 'Sep'):
        return '09'
    elif(m == 'Oct'):
        return '10'
    elif(m == 'Nov'):
        return '11'
    elif(m == 'Dec'):
        return '12'
    else:
        return(np.NaN)

def convGEODate(d):
    if(type(d) == str):
        mon = strToMonth(d[0:3])
        day = d[4:6]
        yr = d[8:12]
        return yr + '-' + mon + '-' + day
    else:
        return np.nan


# In[30]:


# import GEO attribute data and add Series column
geoPlatforms = pd.read_csv('../data/geo_platforms.csv')
geoPlatforms.rename(columns={'Accession':'Platform'}, inplace = True)
techByPlatform = geoPlatforms[['Platform', 'Technology']]

# allData contains metadata matched to GEO series, but lacks 'Technology' column
geoAttributes = pd.merge(allData, techByPlatform, how = 'left', on = 'Platform')
# geoAttributes


# In[ ]:


# convert GEO dates to universal format (both sample-level and series-level)
dates_x = []
for i in geoAttributes['Release Date_x']:
    dates_x.append(convGEODate(i))
geoAttributes['Release Date_x'] = dates_x

dates_y = []
for i in geoAttributes['Release Date_y']:
    dates_y.append(convGEODate(i))
geoAttributes['Release Date_y'] = dates_y
# geoAttributes


# In[ ]:


# Add a column tagging each accession as GEO or SRA
# E-GEOD (ArrayExpress mirrors of GEO) are treated as GEO

repoList = []

for i in pmcData['converted_accession']:
    if(type(i) == str):
        if('GSE' in i or 'GPL' in i or i.startswith('E-G')):
            repoList.append('GEO')
        elif('SRP' in i or 'ERP' in i or 'DRP' in i):
            repoList.append('SRA')
        else:
            repoList.append(np.nan)
    else:
        repoList.append(np.NaN)

pmcData['repository'] = repoList
# pmcData


# In[33]:


# add column for paper publish date
pmc_dates = pd.read_csv('../data/pre_filter_dates.csv')
pmc_dates['pmc_ID'] = pmc_dates['File Name'].apply(lambda x: x.split('/')[-1].split('.')[0])
pmc_dates = pmc_dates[['pmc_ID', 'Date']]
pmc_dates.sample(5)


# In[34]:


pmcData = pd.merge(pmcData, pmc_dates, how = 'left', on = 'pmc_ID')
pmcData = pmcData.rename(columns = {'date': 'pmc_date'})

pmcData.sample(5)


# In[ ]:


# Get every factor we're interested in from our tables of GEO and SRA metadata...

# take a slice of the GEO and SRA attribute tables with only the info we want
# use sample-level release date (Release Date_x), fall back to series-level (Release Date_y)
# for the ~10k series that have no samples in geo_samples.csv
geoAttributes['geoRelease_combined'] = geoAttributes['Release Date_x'].fillna(geoAttributes['Release Date_y'])
null_before = geoAttributes['Release Date_x'].isna().sum()
null_after = geoAttributes['geoRelease_combined'].isna().sum()
print(f'Recovered {null_before - null_after:,} null GEO release dates via series-level fallback')
print(f'Still null: {null_after:,}')

slicedGEOAtt = geoAttributes[['Series', 'geoRelease_combined', 'Technology', 'Taxonomy_x']]
slicedGEOAtt.columns = ['converted_accession', 'geoRelease', 'geoHardware', 'geoSpecies']
slicedGEOAtt = slicedGEOAtt.drop_duplicates(subset = ['converted_accession'])

slicedSRAAtt = sraAttributes[['SRAStudy', 'ReleaseDate', 'Model', 
                              'LibraryStrategy', 'ScientificName', 
                              'bases', 'avgLength', 'Consent']]
slicedSRAAtt.columns = ['converted_accession', 'sraRelease', 'sraHardware', 
                        'sraLibrary_strategy', 'sraSpecies', 
                        'sraBases', 'sraAvg_length', 'sraAccess']
slicedSRAAtt = slicedSRAAtt.drop_duplicates(subset = ['converted_accession'])


# In[36]:


# special case for GEO: make an educated guess on library strategy based on hardware
# These guesses are based on manually checking GEO series IDs that corresponded to various types of hardware

gc = Counter(geoAttributes['Technology'])
ls_guesses = pd.DataFrame.from_dict(gc, orient='index').reset_index()
ls_guesses.columns = ['hardware', 'use_count']
ls_guesses = ls_guesses.drop(labels = ['use_count'], axis = 1)

ls = []

for i in ls_guesses['hardware']:
    if(i == 'high-throughput sequencing'):
        ls.append('RNA-Seq')
    elif(i == 'SAGE NlaIII' or i == 'spotted DNA/cDNA' or i == 'SAGE Sau3A' 
         or i == 'in situ oligonucleotide' or i == 'spotted oligonucleotide' 
         or i == 'antibody' or i == 'MPSS' or i == 'oligonucleotide beads' 
         or i == 'RT-PCR' or i == 'mixed spotted oligonucleotide/cDNA' 
         or i == 'spotted peptide or protein'):
        ls.append('Expression_Array')
    else:
        ls.append(np.nan)

ls_guesses.loc[:,'geoLibrary_strategy'] = ls
# ls_guesses


# In[37]:


# merge SRA attributes onto pmcData table
mergedSRA = pd.merge(pmcData, slicedSRAAtt, how = 'left', on = 'converted_accession')
mergedSRA = mergedSRA.drop_duplicates()

# merge GEO attributes onto table of pmcData + SRA Attributes
allFactors = pd.merge(mergedSRA, slicedGEOAtt, how = 'left', on = 'converted_accession')
allFactors = pd.merge(allFactors, ls_guesses, how = 'left', left_on = 'geoHardware', right_on = 'hardware')

allFactors = allFactors.dropna(subset = ['converted_accession'])


# In[38]:


# clean up columns with factor for both SRA and GEO, rearrange columns
allFactors['species'] = allFactors['sraSpecies'].fillna(allFactors['geoSpecies'])
allFactors = allFactors.drop(labels = ['sraSpecies', 'geoSpecies'], axis = 1)

allFactors['hardware'] = allFactors['sraHardware'].fillna(allFactors['geoHardware'])
allFactors = allFactors.drop(labels = ['sraHardware', 'geoHardware'], axis = 1)

allFactors['library_strategy'] = allFactors['sraLibrary_strategy'].fillna(allFactors['geoLibrary_strategy'])
allFactors = allFactors.drop(labels = ['sraLibrary_strategy', 'geoLibrary_strategy'], axis = 1)

allFactors['repository_date'] = allFactors['sraRelease'].fillna(allFactors['geoRelease'])
allFactors = allFactors.drop(labels = ['sraRelease', 'geoRelease'], axis = 1)


# In[39]:


allFactors.head(5)


# In[40]:


cols = ['journal', 'pmc_ID', 'accession', 'converted_accession', 'repository', 
        'Date', 'repository_date', 'species', 
        'hardware', 'library_strategy', 'sraAvg_length', 'sraBases', 'sraAccess']

allFactors = allFactors[cols]
allFactors


# In[41]:


# deduplicate before saving — same paper can appear in multiple archive files
before = len(allFactors)
allFactors = allFactors.drop_duplicates(subset=['pmc_ID', 'converted_accession'])
print(f'Dropped {before - len(allFactors):,} duplicate pmc+accession rows ({before:,} → {len(allFactors):,})')

# save to .csv
allFactors.to_csv('../data/metadata_matrix_raw.csv', index = False)


# In[ ]:




