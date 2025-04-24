from airflow.decorators import dag, task
from datetime import datetime
import logging
import psycopg2
import pandas as pd
import numpy as np

from tasks.models.xgboost import xgboost_model
from tasks.models.randomforest import randomforest_model
from tasks.models.lightgbm import lightgbm_model
from tasks.models.linear import linear_regression
from tasks.models.arima import arima_model

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 1, 1),
}

@dag(
    dag_id='housing_sg__ml_training_monthly',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    tags=['housing', 'ml', 'training'],
    description='Train ML model monthly on HDB resale data',
)

def ml_training_pipeline():

    @task
    def load_housing_data():
        file_path = '/opt/airflow/data/housing_data.csv'

        logging.info("Loading housing data...")

        logging.info("Connecting to PostgreSQL and loading data...")

        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="airflow",
            user="airflow",
            password="airflow"
        )

        with conn:
            with conn.cursor() as cur, open(file_path, 'w') as f:
                logging.info("Starting data export...")
                cur.copy_expert(
                    sql="COPY housing_data TO STDOUT WITH CSV HEADER DELIMITER ','",
                    file=f
                )
                logging.info("Data export complete.")

        conn.close()

        logging.info(f"housing_data exported to CSV successfully at {file_path}.")

        return file_path
    
    @task
    def load_propertyguru_data():
        output_path = '/opt/airflow/data/propertyguru.csv'

        logging.info("Loading propertyguru data...")

        logging.info("Connecting to PostgreSQL and loading data...")

        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="airflow",
            user="airflow",
            password="airflow"
        )

        with conn:
            with conn.cursor() as cur, open(output_path, 'w') as f:
                cur.copy_expert(
                    sql="COPY propertyguru_data TO STDOUT WITH CSV HEADER DELIMITER ','",
                    file=f
                )

        conn.close()
        logging.info(f"propertyguru_listings exported to {output_path}")
        return output_path

    
    @task
    def preprocess_data(file_path):
        logging.info(f"Preprocessing data from: {file_path}")

        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} rows for preprocessing.")

        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
        df = df[df['transaction_date'].dt.year >= 2002]
        logging.info(f"Filtered for transactions from 2002 onwards. Remaining rows: {len(df)}")

        Q1 = df['price'].quantile(0.25)
        Q3 = df['price'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 3 * IQR
        upper_bound = Q3 + 3 * IQR

        df_filtered = df[(df['price'] >= lower_bound) & (df['price'] <= upper_bound)]

        logging.info(f"Removed outliers. Remaining rows: {len(df_filtered)}")

        df_filtered.to_csv(file_path, index=False)
        logging.info(f"Overwritten original file with cleaned data: {file_path}")
        return file_path
    
    @task
    def preprocess_test_data(file_path):
        logging.info(f"Preprocessing test data from: {file_path}")
        df = pd.read_csv(file_path)
        df = df.drop(columns=['storey_range'])
        logging.info("'storey_range' column dropped.")
        
        df.to_csv(file_path, index=False)
        logging.info(f"Cleaned data saved to: {file_path}")
        return file_path

    housing_file_path = load_housing_data()
    pg_file_path = load_propertyguru_data()
    cleaned_housing = preprocess_data(housing_file_path)
    cleaned_pg = preprocess_test_data(pg_file_path)
    
    #Apply models
    xgboost_model(cleaned_housing, cleaned_pg)
    randomforest_model(cleaned_housing, cleaned_pg)
    lightgbm_model(cleaned_housing, cleaned_pg)
    linear_regression(cleaned_housing, cleaned_pg)
    arima_model(cleaned_housing, cleaned_pg)
    
ml_training_dag = ml_training_pipeline()
