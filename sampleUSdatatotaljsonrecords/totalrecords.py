import json
import pandas as pd
import os

from datetime import datetime, timedelta
import os
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
import zipfile
from io import BytesIO
import csv
import io

# Azure Blob Storage credentials
account_name = 'glstoragefiles'
account_key = '1/RWSRHypoPnScgOtqPrfbvDBN2ffwXE3+9AzHo4MAJOgkbpqVan5+mkQVgPpdcO/qRardZrXtyW+AStG3/lJQ=='
container_name = 'dev-server-blob'

# Connection string
connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_client = blob_service_client.get_container_client(container_name)

# Function to list files inside the 'container_metadata' folder
def list_files_in_metadata_folder():
    print("Files inside 'container_metadata' folder:")
    # Iterate through blobs in the container
    for blob in container_client.list_blobs(name_starts_with='container_metadata/'):
        # Extract the filename from the full blob path
        filename = blob.name.split('/')[-1]
        print(filename)

# Call the function to list files inside the 'container_metadata' folder
list_files_in_metadata_folder()


# Function to handle zip files
def process_zip(blob_name, blob_location):
    sas_token = generate_blob_sas(account_name=account_name,
                                  container_name=container_name,
                                  blob_name=blob_name,
                                  account_key=account_key,
                                  permission=BlobSasPermissions(read=True),
                                  expiry=datetime.utcnow() + timedelta(hours=1))

    # Read zip file contents directly from Azure Blob Storage
    with zipfile.ZipFile(BytesIO(container_client.get_blob_client(blob_name).download_blob().read())) as z:
        file_names = z.namelist()
        if not file_names:
            print(f"No files found in {blob_name}")
            return

        print("Files in ZIP:")
        for file_name in file_names:
            print(file_name)


    #-----------------------------------------------------------------------------------------------------------------------------------
    # Step 1: Define file name and location
    file_name = os.path.basename(blob_name) 
    file_locations = blob_location.name

    #-----------------------------------------------------------------------------------------------------------------------------------
    # Step 2: Data Loading
    with z.open(file_name) as file:
        # Read the CSV file directly from the zip archive
        results = pd.read_csv(file, encoding='latin1')

    #-----------------------------------------------------------------------------------------------------------------------------------
    # Step 3: Data Exploration
    total_rows = len(results)
    print("Number of rows:", total_rows)
    print("Number of columns:", len(results.columns))

    #-----------------------------------------------------------------------------------------------------------------------------------
    # Step 4: Handle Missing Values
    # Example: Fill missing values with empty strings
    results.fillna('', inplace=True)

    #-----------------------------------------------------------------------------------------------------------------------------------
    # Step 5: Convert to JSON
    cleaned_data_json = results.to_json(orient='records')

    #-----------------------------------------------------------------------------------------------------------------------------------
    # Step 6: Save JSON to a text file
    with open('cleaned_data.txt', 'w') as f:
        parsed_json = json.loads(cleaned_data_json)
        json.dump(parsed_json, f, indent=4)

    print("Cleaned data saved to 'cleaned_data.txt'")

    #-----------------------------------------------------------------------------------------------------------------------------------
    # Step 7: Count column values and Convert JSON data to DataFrame
    cleaned_data_df = pd.DataFrame(parsed_json)

    # Count filled values in each column
    filled_counts = cleaned_data_df.apply(lambda col: col[col != ''].count())
    filled_counts_dict = [{"column_name": col, "count": count} for col, count in filled_counts.items()]  # list comprehension

    #-----------------------------------------------------------------------------------------------------------------------------------
    # Step 8: Save filled counts to a JSON file along with file information
    output_dict = {
        "filename": file_name,
        "filename_location": file_locations,
        "total_row_count": total_rows,
        "columns": filled_counts_dict
    }

    with open('filled_value_counts.json', 'w') as f:
        json.dump(output_dict, f, indent=4)

    print("Filled value counts saved to 'filled_value_counts.json'")



# Iterate through blobs in the container
for blob in container_client.list_blobs():
    if blob.name.endswith('.zip'):
        process_zip(blob.name, blob)
    if blob.name.endswith('.csv'):
        print("Files in CSV:",blob.name)
