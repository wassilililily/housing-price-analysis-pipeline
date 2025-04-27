from airflow.decorators import dag, task
from datetime import datetime

from tasks.propertyguru.extract_pg import extract_propertyguru
from tasks.propertyguru.transform_pg import transform_propertyguru
from tasks.propertyguru.load_pg import load_propertyguru_data
from tasks.propertyguru.transform_merge_pg import transform_merge_propertyguru

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 1, 1),
}

@dag(
    dag_id='propertyguru__etl__2h',
    default_args=default_args,
    schedule_interval='0 */2 * * *',
    catchup=False,
    tags=['propertyguru', 'etl'],
    description='ETL pipeline for PropertyGuru data every 2 hours',
)

def propertyguru_etl():
    listings = extract_propertyguru()
    transformed_listings = transform_propertyguru(listings)
    inserted_listing_ids = load_propertyguru_data(transformed_listings)
    transform_merge_propertyguru(inserted_listing_ids)

propertyguru_etl = propertyguru_etl()
