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
from tasks.ura.transform_ura import transform_ura
from tasks.ura.load_ura import load_ura

from tasks.transform_load import transform_merge_data

from tasks.utils.export_housing_data import export_data 
from tasks.utils.export_clean_housing_data import export_clean_housing_data

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

    # SingStat ETL
    file_names = search_files()
    folder_path_and_files = download_files(file_names)

    monthly_output = process_monthly_data(folder_path_and_files)
    quarterly_output = process_quarterly_data(folder_path_and_files)
    annual_basic_output = process_annual_basic_data(folder_path_and_files)
    annual_cpi_output = process_annual_cpi_categories(folder_path_and_files)

    # URA ETL
    ura_path = '/opt/airflow/dags/tasks/ura/URA.xlsx'
    transformed_ura_paths = transform_ura(ura_path)
    
    load_hdb_task = load_hdb(transformed_hdb_path)
    load_monthly_task = load_monthly_data(monthly_output)
    load_quarterly_task = load_quarterly_data(quarterly_output)
    load_annual_basic_task = load_annual_basic_data(annual_basic_output)
    load_annual_cpi_task = load_annual_cpi_categories(annual_cpi_output)
    load_ura_task = load_ura(transformed_ura_paths)

    # set dependency
    merge_task = transform_merge_data()
    
    [
        load_hdb_task,
        load_monthly_task,
        load_quarterly_task,
        load_annual_basic_task,
        load_annual_cpi_task,
        load_ura_task
    ] >> merge_task >> export_data() >> export_clean_housing_data()

housing_etl_pipeline = housing_etl_pipeline()
