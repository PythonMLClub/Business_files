import pandas as pd
import os

# List of CSV files
file_names = [
    "STATE-IA-KY.FOIA.NA.PMT23.FINAL.DT24006.csv",
    "STATE-LA-MT.FOIA.NA.PMT23.FINALDT24006.csv",
    "STATE-OR-TX.FOIA.NA.PMT23.FINAL.DT24006.csv",
    "STATE-NE-OK.FOIA.NA.PMT23.FINAL.DT24006.csv",
    "STATE-UT-WY.FOIA.NA.PMT23.FINAL.DT24006.csv"
]

# Folder containing modified CSV files
output_folder = "Modified_CSV_Files"

# Function to modify CSV files
def modify_csv(file_names, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    modified_files = []
    for file in file_names:
        # Read CSV file
        df = pd.read_csv(file, low_memory=False)
        
        # Modify 'County FSA Code' column
        df['County FSA Code'] = df['County FSA Code'].astype(str).str.zfill(3)
        

        # Modify 'Zip Code' column
        df['Zip Code'] = df['Zip Code'].astype(str)
        df['Zip Code'] = df['Zip Code'].apply(lambda x: (x.zfill(9)[:5] + '-' + x.zfill(9)[5:]) if len(x) >= 6 else x.zfill(5))


         
        # Modify 'Delivery Point Bar Code' column
        df['Delivery Point Bar Code'] = df['Delivery Point Bar Code'].astype(str).str.split('.').str[0].str.zfill(3)

        # Format 'Disbursement Amount' column
        df['Disbursement Amount'] = df['Disbursement Amount'].apply(lambda x: f"${x:.2f}")
        
        # Extract file name without extension
        file_name = os.path.splitext(os.path.basename(file))[0]
        
        # Create modified CSV file name
        modified_csv_file = os.path.join(output_folder, f"{file_name}_modified.csv")
        modified_files.append(modified_csv_file)
        
        # Save modified DataFrame to CSV
        df.to_csv(modified_csv_file, index=False)
        print(f"Modified CSV file '{modified_csv_file}' created successfully.")
    
    return modified_files

# Function to merge CSV files
def merge_csv_files(file_names, output_csv):
    all_data = pd.concat((pd.read_csv(file) for file in file_names), ignore_index=True)
    all_data.to_csv(output_csv, index=False)
    print(f"Merged CSV file '{output_csv}' created successfully.")

# Modify CSV files
modified_files = modify_csv(file_names, output_folder)

# Merge modified CSV files into a single CSV
merge_csv_files(modified_files, "USDA2024NameAddress.csv")
