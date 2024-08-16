import os
import pandas as pd

from pprint import pprint

# Define the folder containing the CSV files and the output file path
input_folder = '/home/nicolaedrabcinski/research/lab/new_reuse/data/sra_complete_runs/'
output_file = '/home/nicolaedrabcinski/research/lab/new_reuse/data/sra_complete_runs.csv'


cur_dir = os.path.dirname(os.path.abspath(__file__))

all_files = [os.path.join(cur_dir, input_folder, filename) for filename in os.listdir(os.path.join(cur_dir, input_folder)) if filename.endswith(".csv")]
# pprint(all_files)

all_dfs = []
count = 0

# Считываем все CSV файлы и добавляем их DataFrame в список
for file_path in all_files:
    print(file_path)
    print(count)
    # Проверяем, что файл не пустой
    file_size = os.path.getsize(file_path)
    if file_size > 0:
        try:
            df = pd.read_csv(file_path, engine='c')
            all_dfs.append(df)
        except pd.errors.ParserError:
            print(f"Error reading file: {file_path}")
    count += 1

# Если есть хотя бы один DataFrame в списке, объединяем их
if all_dfs:
    merged_data = pd.concat(all_dfs, ignore_index=True)
    
    # Сохраняем объединенные данные в новый CSV файл
    merged_data.to_csv(output_file, index=False)
    print('Merged CSV saved successfully.')
else:
    print('No data to merge.')




# # Get a list of all CSV files in the input folder
# csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

# # Initialize an empty list to store DataFrames
# dfs = []

# # Loop through the list of CSV files and read each one into a DataFrame
# for file in csv_files:
#     file_path = os.path.join(input_folder, file)
#     df = pd.read_csv(file_path)
#     dfs.append(df)

# # Concatenate all DataFrames into one
# combined_df = pd.concat(dfs, ignore_index=True)

# # Save the concatenated DataFrame as a single CSV file
# combined_df.to_csv(output_file, index=False)

# print(f"All CSV files have been concatenated and saved to {output_file}")
