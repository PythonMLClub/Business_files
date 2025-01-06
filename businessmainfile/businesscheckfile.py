import os
import csv
import json
import zipfile
from io import BytesIO
from collections import defaultdict
from azure.storage.blob import BlobServiceClient

# Initialize Azure Blob Storage client
account_name = 'glstoragefiles'
account_key = '1/RWSRHypoPnScgOtqPrfbvDBN2ffwXE3+9AzHo4MAJOgkbpqVan5+mkQVgPpdcO/qRardZrXtyW+AStG3/lJQ=='
container_name = 'goleadsinternalfiles'
connect_str = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

# Function to retrieve ZIP file contents from Azure Blob Storage
def get_zip_data(zip_blob_name):
    try:
        blob_client = container_client.get_blob_client(zip_blob_name)
        zip_data = blob_client.download_blob().readall()
        return zip_data
    except Exception as e:
        print(f"Error accessing ZIP file '{zip_blob_name}': {e}")
        return None

# Function to read CSV file and count occurrences of each column
def analyze_csv_data(csv_data):
    column_counts = defaultdict(int)
    total_row_count = 0
    total_column_count = 0

    csv_reader = csv.DictReader(csv_data.splitlines())
    for row in csv_reader:
        total_row_count += 1
        total_column_count = max(total_column_count, len(row))
        for column_name in row:
            column_counts[column_name] += 1

    return {
        "total_row_count": total_row_count,
        "total_column_count": total_column_count,
        "columns": [{"column_name": col, "count": count} for col, count in column_counts.items()]
    }

# Function to write analysis result to JSON
def write_to_json(csv_data, csv_filename):
    analysis_result = analyze_csv_data(csv_data)

    output_directory = 'json_output'
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    json_data = {
        "filename": csv_filename,
        "total_row_count": analysis_result["total_row_count"],
        "total_column_count": analysis_result["total_column_count"],
        "columns": analysis_result["columns"]
    }

    json_file_name = csv_filename.split('.')[0] + '.json'
    json_file_path = os.path.join(output_directory, json_file_name)
    with open(json_file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)
    print(f"JSON file saved: {json_file_path}")

# Specify the path to the outer ZIP file containing the nested ZIP file
#outer_zip_blob_name = 'SourceFiles/BusinessFiles/ListServiceDirect/BusinessFiles_Apr2024/Businessfilesample.zip'
outer_zip_blob_name = 'SourceFiles/BusinessFiles/ListServiceDirect/BusinessFiles_Apr2024/ListServiceDirect_BusinessFiles_Apr2024.zip'

# Retrieve the ZIP file data
outer_zip_data = get_zip_data(outer_zip_blob_name)
if outer_zip_data:
    # Use ZipFile to iterate through the contents of the outer ZIP file
    with zipfile.ZipFile(BytesIO(outer_zip_data), 'r') as outer_zip:
        # Check if the nested ZIP file is present in the outer ZIP file
        nested_zip_file = 'DMI_202403.zip'
        if nested_zip_file in outer_zip.namelist():
            # Read the nested ZIP file data
            nested_zip_data = outer_zip.read(nested_zip_file)
            # Extract the nested ZIP file contents
            with zipfile.ZipFile(BytesIO(nested_zip_data), 'r') as nested_zip:
                # Check if the CSV file is present in the nested ZIP file
                csv_file_name = 'DMI_202403.csv'
                if csv_file_name in nested_zip.namelist():
                    # Read the CSV data using 'latin-1' encoding
                    csv_data = nested_zip.read(csv_file_name).decode('latin-1')
                    print("CSV read successfully....")
                    
                    # Save analysis result as JSON
                    write_to_json(csv_data, csv_file_name)
                else:
                    print(f"CSV file '{csv_file_name}' not found in the nested ZIP file.")
        else:
            print(f"Nested ZIP file '{nested_zip_file}' not found in the outer ZIP file.")
else:
    print("Failed to retrieve ZIP contents.")
