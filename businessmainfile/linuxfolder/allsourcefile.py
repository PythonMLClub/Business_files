import zipfile
import io
import concurrent.futures
import logging
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from azure.storage.blob import BlobServiceClient

# Initialize Azure Blob Storage client
account_name = 'glstoragefiles'
account_key = '1/RWSRHypoPnScgOtqPrfbvDBN2ffwXE3+9AzHo4MAJOgkbpqVan5+mkQVgPpdcO/qRardZrXtyW+AStG3/lJQ=='
container_name = 'goleadsinternalfiles'
connect_str = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Function to fetch filenames from a zip file in chunks
def fetch_filenames_from_zip_chunk(blob_name, start_range, end_range):
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob(
            start_range=start_range,
            end_range=end_range
        ).readall()

        with io.BytesIO(blob_data) as chunk_data:
            with zipfile.ZipFile(chunk_data, 'r') as zip_ref:
                return zip_ref.namelist()
    except (ResourceNotFoundError, HttpResponseError) as e:
        logging.error(f"Error fetching filenames from zip file '{blob_name}': {e}")
        return []
    except Exception as e:
        logging.error(f"An error occurred while fetching filenames from zip file '{blob_name}': {e}")
        return []

# Function to fetch filenames from the entire zip file in chunks and combine the results
def fetch_filenames_from_zip(blob_name):
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_size = blob_client.get_blob_properties().size
        chunk_size = 100 * 1024 * 1024  # 10 MB (increased chunk size)
        filenames = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:  # Increased max_workers
            futures = []
            for start in range(0, blob_size, chunk_size):
                end = min(start + chunk_size - 1, blob_size - 1)
                futures.append(executor.submit(fetch_filenames_from_zip_chunk, blob_name, start, end))
                
            for future in concurrent.futures.as_completed(futures):
                filenames.extend(future.result())

        return filenames
    except (ResourceNotFoundError, HttpResponseError) as e:
        logging.error(f"Error fetching filenames from zip file '{blob_name}': {e}")
        return []
    except Exception as e:
        logging.error(f"An error occurred while fetching filenames from zip file '{blob_name}': {e}")
        return []

# Function to fetch filenames from multiple zip files in parallel
def fetch_filenames_in_parallel(zip_filenames):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(fetch_filenames_from_zip, zip_filenames)
    return dict(zip(zip_filenames, results))

# Specify the zip file names
zip_filenames = [
    'SourceFiles/BusinessFiles/ListServiceDirect/BusinessFiles_Apr2024/BusinessDBFilesSample_Apr2024_forpython.zip',
    'SourceFiles/BusinessFiles/ListServiceDirect/BusinessFiles_Apr2024/Businessfilesample.zip',
    'SourceFiles/BusinessFiles/ListServiceDirect/BusinessFiles_Apr2024/ListServiceDirect_BusinessFiles_Apr2024.zip'
]

try:
    # Fetch filenames from the specified zip files in parallel
    filenames_in_zip = fetch_filenames_in_parallel(zip_filenames)
    
    # Print the filenames
    for zip_filename, filenames in filenames_in_zip.items():
        logging.info(f"Filenames inside the zip file '{zip_filename}':")
        for filename in filenames:
            logging.info(filename)

except ResourceNotFoundError:
    logging.error(f"One or more zip files not found.")
except Exception as e:
    logging.error(f"An error occurred: {e}")