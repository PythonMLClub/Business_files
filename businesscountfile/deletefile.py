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


# Get BlobClient for the business.csv file and delete it
blob_client = blob_service_client.get_blob_client(container=container_name, blob='business.csv')
blob_client.delete_blob()
print("Deleted business.csv from Azure Blob Storage")