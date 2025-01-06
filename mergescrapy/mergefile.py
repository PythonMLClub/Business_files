import pandas as pd
import os

# List all files in the directory
files = os.listdir('.')
csv_files = [file for file in files if file.endswith('.csv')]

# Read each CSV file and concatenate them
dfs = []
for file in csv_files:
    df = pd.read_csv(file)
    dfs.append(df)

# Merge dataframes
merged_df = pd.concat(dfs, ignore_index=True)

# Write merged dataframe to a new CSV file
merged_df.to_csv('columnnamemodify.csv', index=False, na_rep='nan')

print("Merged CSV file 'scrapy.csv' created successfully.")
