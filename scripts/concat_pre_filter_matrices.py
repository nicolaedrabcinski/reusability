import os
import pandas as pd

# Define the folder containing the CSV files and the output file path
input_folder = '/home/nicolaedrabcinski/research/lab/new_reuse/data/pre_filter_matrices/'
output_file = '/home/nicolaedrabcinski/research/lab/new_reuse/data/pre_filter_matrix.csv'

# Get a list of all CSV files in the input folder
csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

# Initialize an empty list to store DataFrames
dfs = []

# Loop through the list of CSV files and read each one into a DataFrame
for file in csv_files:
    file_path = os.path.join(input_folder, file)
    df = pd.read_csv(file_path)
    dfs.append(df)

# Concatenate all DataFrames into one
combined_df = pd.concat(dfs, ignore_index=True)

# Save the concatenated DataFrame as a single CSV file
combined_df.to_csv(output_file, index=False)

print(f"All CSV files have been concatenated and saved to {output_file}")
