from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import os


with DAG(
    dag_id='clean_housing_data',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['housing', 'cleaning'],
) as dag:

    @task
    def clean_and_save_data():
        input_path = '/opt/airflow/data/housing_data.csv'
        output_path = '/opt/airflow/data/cleaned_housing_data.csv'


        df = pd.read_csv(input_path)
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

        
        floor_min = df['floor_area_sqm'].quantile(0.005)
        floor_max = df['floor_area_sqm'].quantile(0.995)


        price_min = df['price'].quantile(0.005)
        price_max = df['price'].quantile(0.995)


        df_cleaned = df[
            (df['floor_area_sqm'] >= floor_min) &
            (df['floor_area_sqm'] <= floor_max) &
            (df['price'] >= price_min) &
            (df['price'] <= price_max)
        ].copy()

        df_cleaned.to_csv(output_path, index=False)
       
        print(f"Original: {len(df)}, Cleaned: {len(df_cleaned)}")

    clean_and_save_data()
