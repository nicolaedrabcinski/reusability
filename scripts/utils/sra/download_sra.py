import csv
import subprocess
import os
import glob

from tqdm import tqdm

interim_pfm_data = "/home/nicolaedrabcinski/research/lab/new_reuse/data/pre_filter_matrices/"
interim_scr_data = "/home/nicolaedrabcinski/research/lab/new_reuse/data/sra_complete_runs/"
processed_pfm_data = "/home/nicolaedrabcinski/research/lab/new_reuse/data/pre_filter_matrix.csv"

pfm_csv_files = glob.glob(os.path.join(interim_pfm_data, '*.csv'))
pfm_csv_files_sorted = sorted(pfm_csv_files, key=lambda x: os.path.getsize(x))

counter = 0

for csv_file in pfm_csv_files_sorted:
    print("Processing CSV file: ", csv_file)

    already_run_path = os.listdir(interim_scr_data)
    already_runs = []
    
    
    for file_name in already_run_path:
        filtered_file_name = file_name.replace("output_", "").replace(".csv", "")
        already_runs.append(filtered_file_name)

    counter = len(already_runs)
    
    print("Processed SRA runs : ", len(already_runs))

    values = []

    with open(csv_file, "r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Пропустить заголовок
        for row in csv_reader:
            values.append(row[2])

    print("Found accessions : ", len(values))
    values = list(dict.fromkeys(values))
    print("Found unique accessions : ", len(values))

    filtered_runs = list(set(values) - set(already_runs))
    print("Accessions to download : ", len(filtered_runs))

    if len(filtered_runs) == 0:
        print("There are no unprocessed accessions in file : \n", csv_file)
        continue

    with tqdm(total=len(filtered_runs), desc='Processing accession', unit='accession') as pbar:
        # Цикл для обработки каждого значения
        for value in filtered_runs:
            counter += 1
            print("Processing accession : ", value)
        
            search_command = f"esearch -db sra -query {value}"
            fetch_command = "efetch -format runinfo"
            subprocess.run(f"{search_command} | {fetch_command} > {interim_scr_data}/output_{value}.csv", shell=True)
            print("Globally processed accessions : ", counter)
            # Обновление прогресс-бара
            pbar.update(1)