from airflow.decorators import task
import requests
import logging
import pandas as pd

from io import StringIO

@task
def extract_hdb_resale():
    collection_id = 189
    url = "https://api-production.data.gov.sg/v2/public/api/collections/{}/metadata".format(collection_id)

    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"Failed to fetch metadata: {response.status_code}")
        return []
    
    collection = response.json()
    
    child_datasets = (
        collection.get("data", {})
        .get("collectionMetadata", {})
        .get("childDatasets", [])
    )
    logging.info(child_datasets)

    hdb_resale_data = []
    for dataset_id in child_datasets:
        logging.info(f"Retrieving info from {dataset_id}")

        dataset_url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download"

        try:
            dataset_response = requests.get(dataset_url, headers={"Content-Type": "application/json"}, timeout=10)
            dataset_response.raise_for_status()
        except requests.RequestException as e:
            logging.warning(f"Failed to fetch dataset {dataset_id}: {e}")
            continue

        dataset_info = dataset_response.json()
        dataset_download_url = dataset_info.get("data").get("url")

        if dataset_download_url:
            logging.info(f"Downloading dataset from {dataset_download_url}")
            
            try:
                dataset_file_response = requests.get(dataset_download_url, timeout=20)
                dataset_file_response.raise_for_status()

                # Use StringIO to parse CSV content without saving to disk
                csv_data = StringIO(dataset_file_response.text)
                df = pd.read_csv(csv_data)

                dataset_records = df.to_dict(orient="records")
                hdb_resale_data.extend(dataset_records)

            except requests.RequestException as e:
                logging.warning(f"Failed to download dataset {dataset_download_url}: {e}")
                continue

    logging.info(f"Total records collected: {len(hdb_resale_data)}")
    output_file = "/opt/airflow/data/hdb_resale_data.csv"
    pd.DataFrame(hdb_resale_data).to_csv(output_file, index=False)

    return output_file