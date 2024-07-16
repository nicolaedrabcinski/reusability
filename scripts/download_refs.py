import os
import requests
from bs4 import BeautifulSoup

# Function to get the number of pages dynamically
def get_num_pages(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    pagination = soup.find('div', {'class': 'pagination'})
    if pagination:
        last_page = pagination.find_all('a')[-2].text
        return int(last_page)
    return 1

# Base URL for GEO samples
base_url_samples = 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=samples&sort=date&mode=csv&page='

# Determine the number of pages dynamically
NUM_SAMPLE_PAGES = get_num_pages(base_url_samples + '1&display=5000')

# Prepare the header for the final CSV
os.system("echo 'Accession,Title,Sample_Type,Taxonomy,Channels,Platform,Series,Supplementary_Types,Supplementary_Links,SRA_Accession,Contact,Release_Date' > ./geo_samples.csv")

# Download and concatenate sample data
for i in range(1, NUM_SAMPLE_PAGES + 1):
    url = base_url_samples + str(i) + '&display=5000'
    os.system(f"wget {url} -O ./samples_{i}.csv")
    os.system(f"sed 1d ./samples_{i}.csv >> ./geo_samples.csv")
    os.system(f"rm -f ./samples_{i}.csv")

# Repeat the same process for series and platforms
base_url_series = 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=series&sort=date&mode=csv&page='
NUM_SERIES_PAGES = get_num_pages(base_url_series + '1&display=5000')
os.system("echo 'Accession,Title,Series_Type,Taxonomy,Sample_Count,Datasets,Supplementary_Types,Supplementary_Links,PubMed_ID,SRA_Accession,Contact,Release_Date' > ./geo_series.csv")
for i in range(1, NUM_SERIES_PAGES + 1):
    url = base_url_series + str(i) + '&display=5000'
    os.system(f"wget {url} -O ./series_{i}.csv")
    os.system(f"sed 1d ./series_{i}.csv >> ./geo_series.csv")
    os.system(f"rm -f ./series_{i}.csv")

base_url_platforms = 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=platforms&sort=date&mode=csv&page='
NUM_PLATFORM_PAGES = get_num_pages(base_url_platforms + '1&display=5000')
os.system("echo 'Accession,Title,Technology,Taxonomy,Data_Rows,Samples_Count,Series_Count,Contact,Release_Date' > ./geo_platforms.csv")
for i in range(1, NUM_PLATFORM_PAGES + 1):
    url = base_url_platforms + str(i) + '&display=5000'
    os.system(f"wget {url} -O ./platforms_{i}.csv")
    os.system(f"sed 1d ./platforms_{i}.csv >> ./geo_platforms.csv")
    os.system(f"rm -f ./platforms_{i}.csv")

# Download SRA reference data
sources = ["genomic", "genomic single cell", "metagenomic", "metatranscriptomic", "other", "synthetic", "transcriptomic", "transcriptomic single cell", "viral rna"]
filenames = ["genomic", "genomic_single_cell", "metagenomic", "metatranscriptomic", "other", "synthetic", "transcriptomic", "transcriptomic_single_cell", "viral_rna"]

for i in range(0, len(sources)):
    os.system(f"wget http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=runinfo&term={sources[i]}[Source]' -O ./sra_runs_{filenames[i]}.csv")

os.system("echo 'Run,ReleaseDate,LoadDate,spots,bases,spots_with_mates,avgLength,size_MB,AssemblyName,download_path,Experiment,LibraryName,LibraryStrategy,LibrarySelection,LibrarySource,LibraryLayout,InsertSize,InsertDev,Platform,Model,SRAStudy,BioProject,Study_Pubmed_id,ProjectID,Sample,BioSample,SampleType,TaxID,ScientificName,SampleName,g1k_pop_code,source,g1k_analysis_group,Subject_ID,Sex,Disease,Tumor,Affection_Status,Analyte_Type,Histological_Type,Body_Site,CenterName,Submission,dbgap_study_accession,Consent,RunHash,ReadHash' > ./sra_complete_runs.csv")
for i in range(0, len(sources)):
    os.system(f"sed 1d ./sra_runs_{filenames[i]}.csv >> ./sra_complete_runs.csv")
