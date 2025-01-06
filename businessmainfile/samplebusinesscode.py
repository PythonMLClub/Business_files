import zipfile
import csv
import json
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import numpy as np
import threading

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

# Convert int64 to Python native int for JSON serialization
def convert_int64_to_int(value):
    if isinstance(value, np.int64):
        return int(value)
    return value

# Function to count records in each column
def count_records(csv_reader):
    column_counts = {}
    header = next(csv_reader)  # Read the header
    for idx, column_name in enumerate(header):
        column_counts[column_name] = 0
    
    for row in csv_reader:
        for idx, value in enumerate(row):
            if value.strip():  # Check if the value is not empty
                column_name = header[idx]
                column_counts[column_name] += 1
                
    return column_counts

# Function to process nested ZIP file
def process_nested_zip(zip_data, nested_zip_file):
    with zipfile.ZipFile(BytesIO(zip_data), 'r') as outer_zip:
        nested_zip_data = outer_zip.read(nested_zip_file)
        with zipfile.ZipFile(BytesIO(nested_zip_data), 'r') as nested_zip:
            csv_file_name = 'BusinessDatabase_samplefile_Apr2024.csv'
            if csv_file_name in nested_zip.namelist():
                csv_data = nested_zip.read(csv_file_name).decode('latin-1')
                csv_reader = csv.reader(csv_data.splitlines())
                total_row_count = sum(1 for row in csv_reader)
                csv_data = nested_zip.read(csv_file_name).decode('latin-1')
                csv_reader = csv.reader(csv_data.splitlines())
                column_counts = count_records(csv_reader)
                result = {
                    "filename": csv_file_name,
                    "total_row_count": total_row_count,
                    "total_column_count": len(column_counts),
                    "columns": [{"column_name": column_name, "count": count} for column_name, count in column_counts.items()]
                }
                with open('testresult.json', 'w') as json_file:
                    json.dump(result, json_file, indent=4)
                print("JSON file saved successfully.")
            else:
                print(f"CSV file '{csv_file_name}' not found in the nested ZIP file.")

# Specify the path to the outer ZIP file containing the nested ZIP file
outer_zip_blob_name = 'SourceFiles/BusinessFiles/ListServiceDirect/BusinessFiles_Apr2024/Businessfilesample.zip'

# Retrieve the ZIP file data
outer_zip_data = get_zip_data(outer_zip_blob_name)
if outer_zip_data:
    # Calculate file size before threading
    original_file_size = len(outer_zip_data)
    
    # Define chunk size
    chunk_size = 1024 * 1024  # 1 MB
    
    # Split the ZIP file into chunks
    num_chunks = (len(outer_zip_data) + chunk_size - 1) // chunk_size
    chunks = [outer_zip_data[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]
    
    threads = []
    for chunk in chunks:
        thread = threading.Thread(target=process_nested_zip, args=(chunk, 'BusinessDatabase_samplefile_Apr2024.zip'))
        thread.start()
        threads.append(thread)
    
    # Wait for all threads to finish
    for thread in threads:
        thread.join()
    
    # Calculate file size after threading
    new_outer_zip_data = get_zip_data(outer_zip_blob_name)
    new_file_size = len(new_outer_zip_data)
    
    print(f"Original file size: {original_file_size} bytes")
    print(f"New file size after threading: {new_file_size} bytes")

    original_file_size = len(outer_zip_data)

    # Memory used by chunks
    num_chunks = (original_file_size + chunk_size - 1) // chunk_size
    memory_used = original_file_size + num_chunks * chunk_size

    # Convert to MB
    memory_used_MB = memory_used / (1024 * 1024)

    print(f"Memory used: {memory_used_MB} MB")
else:
    print("Failed to retrieve ZIP contents.")
