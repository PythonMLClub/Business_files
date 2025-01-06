from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd
import os
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

        # Read and process each file
        for file_name in file_names:
            with z.open(file_name) as f:
                if file_name.endswith('.csv'):
                    # Read CSV file
                    csv_reader = csv.DictReader(io.TextIOWrapper(f, encoding='latin1'))
                    records = list(csv_reader)
                    process_and_upload_batches(records)
                    print(f"Successfully processed and uploaded {len(records)} records from {file_name}")
                else:
                    print(f"Skipping file {file_name} as it is not a CSV file.")

def process_and_upload_batches(records):
    batch_size = 1000  # Number of records per batch
    total_records = len(records)
    total_processed_records = 0  # Initialize total processed records counter
    for i in range(0, total_records, batch_size):
        batch_records = records[i:i+batch_size]
        total_processed_records += len(batch_records)  # Increment counter by batch size
        # Create a CSV file for the batch records
        csv_filename = f"batch_{i//batch_size + 1}.csv"
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=batch_records[0].keys())
            writer.writeheader()
            writer.writerows(batch_records)
        print(f"Saved batch records to {csv_filename}")
        upload_csv_to_blob_storage(csv_filename)

    print(f"Total processed records: {total_processed_records}")  # Print total processed records count



def upload_csv_to_blob_storage(csv_filename):
    blob_client = container_client.get_blob_client('business.csv')
    # Check if the blob already exists
    if blob_client.exists():
        # Download existing blob content
        existing_blob = blob_client.download_blob()
        existing_data = existing_blob.readall().decode('utf-8')
        # Read the new CSV file
        with open(csv_filename, 'r', encoding='utf-8') as csv_file:
            new_data = csv_file.read()
        # Combine existing data with new data
        combined_data = existing_data + new_data
        # Upload the combined data back to the same blob
        blob_client.upload_blob(combined_data, overwrite=True)
    else:
        # Read the new CSV file
        with open(csv_filename, 'r', encoding='utf-8') as csv_file:
            data = csv_file.read()
        # Upload data to blob
        blob_client.upload_blob(data)

    print("Appended a batch of records to Azure Blob Storage")


# Iterate through blobs in the container
for blob in container_client.list_blobs():
    if blob.name.endswith('.zip'):
        process_zip(blob.name, blob)
