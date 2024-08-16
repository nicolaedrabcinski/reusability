import os
import pandas as pd
import requests
from io import StringIO
from tqdm import tqdm

# Директории для временного хранения и конечного вывода
local_dir_temp = {
    'samples': '/home/nicolaedrabcinski/research/lab/new_reuse/data/geo_samples/',
    'series': '/home/nicolaedrabcinski/research/lab/new_reuse/data/geo_series/',
    'platforms': '/home/nicolaedrabcinski/research/lab/new_reuse/data/geo_platforms/'
}

output_file = {
    'samples': '/home/nicolaedrabcinski/research/lab/new_reuse/data/geo_samples.csv',
    'series': '/home/nicolaedrabcinski/research/lab/new_reuse/data/geo_series.csv',
    'platforms': '/home/nicolaedrabcinski/research/lab/new_reuse/data/geo_platforms.csv'
}

# URL для загрузки данных
URL_GEO = {
    'samples': 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=samples&sort=date&mode=csv&page={}&display=5000',
    'series': 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=series&sort=date&mode=csv&page={}&display=5000',
    'platforms': 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=platforms&sort=date&mode=csv&page={}&display=5000'
}

# Заголовки для проверки окончания данных
desired_headers = {
    'samples': "Accession,Title,Sample Type,Taxonomy,Channels,Platform,Series,Supplementary Types,Supplementary Links,SRA Accession,Contact,Release Date",
    'series': "Accession,Title,Series Type,Taxonomy,Sample Count,Datasets,Supplementary Types,Supplementary Links,PubMed ID,SRA Accession,Contact,Release Date",
    'platforms': "Accession,Title,Technology,Taxonomy,Data Rows,Samples Count,Series Count,Contact,Release Date"
}

# Функция для загрузки и обработки данных
def download_and_process_data(data_type):
    page_number = 1
    local_dir = local_dir_temp[data_type]
    os.makedirs(local_dir, exist_ok=True)
    
    while True:
        url = URL_GEO[data_type].format(page_number)
        response = requests.get(url)
        
        if response.status_code == 200:
            if response.text.strip() == desired_headers[data_type]:
                print(f"No more data available for {data_type}. Stopping.")
                break

            temp_df = pd.read_csv(StringIO(response.text))
            temp_csvfile_path = os.path.join(local_dir, f'{data_type}_{page_number}.csv')
            temp_df.to_csv(temp_csvfile_path, index=False)
            
            print(f"Processing {data_type} pages: ", page_number)
            page_number += 1
        else:
            print(f"Failed to fetch data from page {page_number} for {data_type}")
            break

    csv_files = [f for f in os.listdir(local_dir) if f.endswith('.csv')]
    combined_df = pd.concat([pd.read_csv(os.path.join(local_dir, csv_file)) for csv_file in csv_files], ignore_index=True)
    
    combined_df.to_csv(output_file[data_type], index=False)

# Обработка данных для каждого типа
for data_type in ['samples', 'series', 'platforms']:
    download_and_process_data(data_type)
