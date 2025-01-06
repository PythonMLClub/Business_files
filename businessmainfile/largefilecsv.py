import zipfile
import io
import concurrent.futures
import logging
import time
import pandas as pd
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
            end_range=end_range,
            timeout=(30, 600)  # Increase the timeout duration (30 seconds for connect, 600 seconds for read)
        ).readall()

        logging.debug(f"Chunk size: {len(blob_data)} bytes")
        with zipfile.ZipFile(io.BytesIO(blob_data), 'r') as zip_ref:
            return zip_ref.namelist()
    except zipfile.BadZipFile as e:
        logging.error(f"Error fetching filenames from zip file '{blob_name}': {e}")
        return []
    except (ResourceNotFoundError, HttpResponseError) as e:
        logging.error(f"Error fetching filenames from zip file '{blob_name}': {e}")
        return []
    except Exception as e:
        logging.error(f"An error occurred while fetching filenames from zip file '{blob_name}': {e}")
        raise

# Function to fetch filenames from the entire zip file in chunks and combine the results
def fetch_filenames_from_zip(blob_name, max_retries=3):
    retries = 0
    while True:
        try:
            blob_client = container_client.get_blob_client(blob_name)
            blob_size = blob_client.get_blob_properties().size
            print("blob_size:", blob_size)
            chunk_size = 30 * 1024 * 1024  # 30 MB (increased chunk size)
            print("chunk_size:", chunk_size)
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
            retries += 1
            if retries > max_retries:
                logging.error(f"Max retries reached for fetching filenames from zip file '{blob_name}'")
                raise
            max_retries += 1  # Increase max retries
            logging.info(f"Retrying after 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logging.error(f"An error occurred while fetching filenames from zip file '{blob_name}': {e}")
            raise

# Function to fetch filenames from a single zip file
def fetch_filenames_from_single_zip(zip_filename):
    try:
        filenames = fetch_filenames_from_zip(zip_filename)
        nested_zip_files = [filename for filename in filenames if filename.endswith('.zip')]
        nested_filenames = []
        for nested_zip_file in nested_zip_files:
            logging.debug(f"Found nested zip file: {nested_zip_file}")
            nested_filenames.extend(fetch_filenames_from_single_zip(nested_zip_file))
        return filenames + nested_filenames
    except Exception as e:
        logging.error(f"An error occurred while processing zip file '{zip_filename}': {e}")
        return []

# Function to fetch filenames from the specified zip files in parallel with retry mechanism
def fetch_filenames_in_parallel(zip_filenames, max_retries=3):
    retries = 0
    while True:
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(fetch_filenames_from_single_zip, zip_filenames)
            return dict(zip(zip_filenames, results))
        except Exception as e:
            retries += 1
            if retries > max_retries:
                print("retries:", retries)
                print("max_retries:", max_retries)
                logging.error("Max retries reached for fetching filenames in parallel")
                raise
            max_retries += 1  # Increase max retries
            print("increase max_retries:", max_retries)
            logging.info(f"Retrying after 5 seconds...")
            time.sleep(5)

# Function to read CSV file using pandas and print total rows and columns
def read_csv_and_print_info(blob_name):
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob(timeout=(30, 600)).readall()  # Increase the timeout duration (30 seconds for connect, 600 seconds for read)

        with zipfile.ZipFile(io.BytesIO(blob_data), 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.csv'):
                    with zip_ref.open(file_info) as csv_file:
                        df = pd.read_csv(csv_file)
                        rows, columns = df.shape
                        logging.info(f"CSV file: {file_info.filename}")
                        logging.info(f"Total rows: {rows}")
                        logging.info(f"Total columns: {columns}")
    except Exception as e:
        logging.error(f"An error occurred while reading CSV file '{blob_name}': {e}")

try:
    # Specify the zip file name
    zip_filename = 'SourceFiles/BusinessFiles/ListServiceDirect/BusinessFiles_Apr2024/DMI_202403.zip'

    # Fetch filenames from the specified zip file
    filenames_in_zip = fetch_filenames_in_parallel([zip_filename])
    
    # Read and print information for CSV files
    for zip_filename, filenames in filenames_in_zip.items():
        for filename in filenames:
            read_csv_and_print_info(filename)

except ResourceNotFoundError:
    logging.error("The zip file was not found.")
except Exception as e:
    logging.error(f"An error occurred: {e}")
