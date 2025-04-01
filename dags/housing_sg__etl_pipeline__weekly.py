from airflow.decorators import dag, task
from datetime import datetime
import logging

from tasks.hdb.extract_hdb import extract_hdb_resale
from tasks.hdb.transform_hdb import transform_hdb
from tasks.hdb.load_hdb import load_hdb

from tasks.singstat.extract_singstat import search_files, download_files
from tasks.singstat.transform_singstat import (
    process_monthly_data,
    process_quarterly_data,
    process_annual_basic_data,
    process_annual_cpi_categories
)
from tasks.singstat.load_singstat import (
    load_monthly_data,
    load_quarterly_data,
    load_annual_basic_data,
    load_annual_cpi_categories
)

from tasks.transform import transform_merge_data

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 1, 1),
}

# Define the DAG with TaskFlow API
@dag(
    dag_id='housing_sg__etl_pipeline__weekly',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    tags=['housing', 'etl', 'singapore'],
    description='ETL pipeline for Singapore housing price and economic data',
)
def housing_etl_pipeline():
    # HDB ETL
    hdb_path = extract_hdb_resale()
    transformed_hdb_path = transform_hdb(hdb_path)
    load_hdb(transformed_hdb_path)

    # # SingStat ETL
    file_names = search_files()
    folder_path_and_files = download_files(file_names)

    monthly_output = process_monthly_data(folder_path_and_files)
    quarterly_output = process_quarterly_data(folder_path_and_files)
    annual_basic_output = process_annual_basic_data(folder_path_and_files)
    annual_cpi_output = process_annual_cpi_categories(folder_path_and_files)

    load_monthly_data(monthly_output)
    load_quarterly_data(quarterly_output)
    load_annual_basic_data(annual_basic_output)
    load_annual_cpi_categories(annual_cpi_output)
    
    transform_merge_data()

housing_etl_pipeline = housing_etl_pipeline()
