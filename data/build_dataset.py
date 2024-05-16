"""
Builds the Ademe dataset from the Ademe API
- gets results (data and next URL) from API
- processes results
- stores data on Azure
"""

import os
import json
from urllib.parse import urlparse, parse_qs
import time
import glob
import requests
from azure.storage.blob import BlobServiceClient

DATA_PATH = "./data/"
API_PATH = "./data/api/"
RESULTS_FILE = os.path.join(API_PATH, "results.json")
ACCOUNT_NAME = "skatai4ademe4mlops"
ACCOUNT_KEY = os.environ.get("STORAGE_BLOB_ADEME_MLOPS")
CONTAINER_NAME = "ademe-dpe-tertiaire"

URL_FILE_BUILD_DATASET = os.path.join(API_PATH, "url.json")
def interrogate_api():
    """
    Interrogates the ADEME API using the specified URL and payload from a JSON file.

    - Reads the URL and payload from a JSON file defined by the constant `URL_FILE`.
    - Performs a GET request to the obtained URL with the given payload.
    - Saves the results to a JSON file defined by the constant `RESULTS_FILE`.

    Raises:
        AssertionError: If the URL file does not exist, or if the retrieved URL or payload is None.
        requests.exceptions.RequestException: If the GET request encounters an error.

    """
    # test url file exists
    assert os.path.isfile(URL_FILE_BUILD_DATASET)
    # open url file
    with open(URL_FILE_BUILD_DATASET, encoding="utf-8") as file:
        url_build_data = json.load(file)
    assert url_build_data.get("url") is not None
    assert url_build_data.get("payload") is not None

    # make GET requests build dataset
    results_build_data = requests.get(
        url_build_data.get("url"),
        params=url_build_data.get("payload"), timeout=5)
    assert results_build_data.raise_for_status() is None

    data = results_build_data.json()

    # save results to file
    with open(RESULTS_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

def process_results():
    """
    Processes the results obtained from the previous API call,
    updates the URL file,
    and saves the data to a new file.

    - Reads the results from a JSON file defined by the constant `RESULTS_FILE`.
    - Extracts the base URL and payload from the 'next' field of the results.
    - Updates the URL file with the same URL and the new payload.
    - Saves the results data to a new JSON file with a filename containing a timestamp.

    Raises:
        AssertionError: If the results file does not exist.

    """
    # test url file exists
    assert os.path.isfile(RESULTS_FILE)

    # read previous API call output
    with open(RESULTS_FILE, encoding="utf-8") as file:
        data = json.load(file)

    # new url is same as old url
    base_url = data.get("next").split("?")[0]

    # extract payload as dict
    parsed_url = urlparse(data.get("next"))
    query_params_build = parse_qs(parsed_url.query)
    new_payload = {k: v[0] if len(v) == 1 else v for k, v in query_params_build.items()}

    # save new url (same as old url) with new payload into url.json
    new_url = {"url": base_url, "payload": new_payload}

    with open(URL_FILE_BUILD_DATASET, "w", encoding="utf-8") as file:
        json.dump(new_url, file, indent=4, ensure_ascii=False)

    # saves data to data file
    # append current timestamp (up to the second to the filename)
    timestamp = int(time.time())
    data_filename = os.path.join(DATA_PATH, f"data_{timestamp}.json")

    with open(data_filename, "w", encoding="utf-8") as file:
        json.dump(data["results"], file, indent=4, ensure_ascii=False)


def upload_data():
    """
    Uploads local data files to Azure Blob Storage container.

    - Establishes a connection to the Azure Blob Storage using the provided account credentials.
    - Retrieves the list of existing blobs in the specified container.
    - Gets a list of local data files to upload.
    - Uploads each local file to the container if it doesn't already exist.

    Environment Variables:
        STORAGE_BLOB_ADEME_MLOPS: Azure Storage account key.

    """

    connection_string = f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT_NAME};"
    connection_string += f"AccountKey={ACCOUNT_KEY};EndpointSuffix=core.windows.net"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_client = blob_service_client.get_container_client(container=CONTAINER_NAME)

    # List all blobs in the container
    blobs_list = [file["name"] for file in container_client.list_blobs()]

    # get all data files on local
    local_data_files = glob.glob(f"{DATA_PATH}/*.json")
    for filename in local_data_files:
        blob_name = filename.split("/")[-1]
        if blob_name not in blobs_list:
            # upload file to container
            blob_client = blob_service_client.get_blob_client(
                container=CONTAINER_NAME,
                blob=blob_name
            )

            print("\nUploading to Azure Storage as blob:\n\t" + blob_name)

            # Upload the created file
            with open(filename, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            print("\nUpload completed")


if __name__ == "__main__":
    interrogate_api()
    process_results()
    # upload_data()
