import csv
import io
import json
import os
import zipfile
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import numpy as np
import pandas as pd

# Initialize Azure Blob Storage client
account_name = 'glstoragefiles'
account_key = '1/RWSRHypoPnScgOtqPrfbvDBN2ffwXE3+9AzHo4MAJOgkbpqVan5+mkQVgPpdcO/qRardZrXtyW+AStG3/lJQ=='
container_name = 'goleadsinternalfiles'
connect_str = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

# Check if the ZIP file exists
zip_file_name = 'SourceFiles/BusinessFiles/ListServiceDirect/BusinessFiles_Apr2024/Businessfilesample.zip'

# Function to check if a blob exists
def blob_exists(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    return blob_client.exists()

# Function to list CSV files inside a ZIP
def list_csv_files_in_zip(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    csv_files = []
    with zipfile.ZipFile(BytesIO(blob_client.download_blob().read())) as z:
        for file_name in z.namelist():
            if file_name.endswith('.csv'):
                csv_files.append((file_name, blob_name + '/' + file_name))  # Append filename and its location
    return csv_files


# Function to ensure the directory exists
def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


# Convert int64 to Python native int for JSON serialization
def convert_int64_to_int(value):
    if isinstance(value, np.int64):
        return int(value)
    return value


def process_csv_file(blob_name, csv_file_name):
    if csv_file_name == 'Businessfilesample/combined.csv':
        blob_client = container_client.get_blob_client(blob_name)
        with zipfile.ZipFile(BytesIO(blob_client.download_blob().read())) as z:
            try:
                with z.open(csv_file_name, 'r') as csv_file:
                    # Attempt to read the CSV file into a pandas DataFrame
                    try:
                        df = pd.read_csv(csv_file, encoding='latin1', engine='python', error_bad_lines=False)
                    except UnicodeDecodeError as e:
                        print(f"Unicode decoding error occurred for file '{csv_file_name}': {str(e)}")
                        return
                    
                    # Get the number of rows and columns
                    num_rows, num_columns = df.shape

                    # Print file information
                    print(f"File name: {csv_file_name}")
                    print(f"Number of rows: {num_rows}")
                    print(f"Number of columns: {num_columns}")
                    
                    # Ensure the directory exists
                    output_directory = 'json_output'
                    ensure_directory_exists(output_directory)

                    # Gather column names from the CSV file
                    csv_column_names = df.columns.tolist()

                    # Initialize column counts with zeros for all columns
                    column_counts = [{"column_name": column_name, "count": 0} for column_name in csv_column_names]

                    # Update counts for existing columns
                    for entry in column_counts:
                        column_name = entry["column_name"]
                        non_nan_non_zero_count = df[column_name].notna().sum() - df[column_name].eq(0).sum()
                        entry["count"] = convert_int64_to_int(non_nan_non_zero_count)

                    # Check if any columns are missing and append them with count 0
                    missing_columns = set(csv_column_names) - set(entry["column_name"] for entry in column_counts)
                    for column_name in missing_columns:
                        column_counts.append({"column_name": column_name, "count": 0})
                    
                    # Construct JSON object
                    json_data = {
                        "filename": csv_file_name,
                        "filename_location": blob_name,
                        "total_row_count": len(df),
                        "total_column_count": num_columns,
                        "columns": column_counts
                    }
                   
                    # Save JSON data to file
                    json_file_name = os.path.basename(csv_file_name).replace('.csv', '.json')
                    json_file_path = os.path.join(output_directory, json_file_name)
                    with open(json_file_path, 'w') as json_file:
                        json.dump(json_data, json_file, indent=4)
                    print(f"JSON file saved: {json_file_path}")
                    
            except pd.errors.ParserError as e:
                print(f"Error parsing CSV file '{csv_file_name}': {str(e)}")
            except Exception as e:
                print(f"Error processing CSV file '{csv_file_name}': {str(e)}")


        
if blob_exists(zip_file_name):
    # List CSV files inside the ZIP with their locations
    csv_files_in_zip = list_csv_files_in_zip(zip_file_name)
    if csv_files_in_zip:
        for csv_file_info in csv_files_in_zip:
            csv_file_name, file_location = csv_file_info
            print("---------------------------------------")
            print(f"File name: {csv_file_name}")
            print(f"Filename_location: {file_location}")
            print("---------------------------------------")
            # Apply further processing if needed
            process_csv_file(zip_file_name, csv_file_name)
    else:
        print("No CSV files found inside the ZIP.")
else:
    print(f"ZIP file '{zip_file_name}' does not exist.")
