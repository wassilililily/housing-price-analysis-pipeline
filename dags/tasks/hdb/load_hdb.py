import pandas as pd
import psycopg2
import logging
from airflow.decorators import task
from datetime import datetime

@task
def load_hdb(transformed_file_path):
    # Read the transformed CSV file
    df = pd.read_csv(transformed_file_path, low_memory=False, parse_dates=['month'])

    if df.empty:
        logging.error("[ERROR] Transformed data is empty!")
        return
    
    # Convert 'month' to date format (removing timestamp)
    df['month'] = df['month'].dt.date  

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Ensure the table exists with correct data types
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hdb_resale_prices (
            id SERIAL PRIMARY KEY,
            month DATE NOT NULL,
            town TEXT NOT NULL,
            flat_type TEXT NOT NULL,
            storey_range TEXT NOT NULL,
            storey_range_continuous FLOAT,
            floor_area_sqm FLOAT NOT NULL,
            lease_commence_date INTEGER NOT NULL,
            remaining_lease TEXT,
            remaining_lease_months FLOAT,
            resale_price FLOAT NOT NULL
        );
    """)
    conn.commit()
    logging.info("[INFO] Table ensured.")

    # Insert data into the table
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO hdb_resale_prices (
                month, town, flat_type, storey_range, storey_range_continuous,
                floor_area_sqm, lease_commence_date, remaining_lease, 
                remaining_lease_months, resale_price
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (
            row['month'],
            row['town'],
            row['flat_type'],
            row['storey_range'],
            row.get('storey_range_continuous'),
            row['floor_area_sqm'],
            row['lease_commence_date'],
            row['remaining_lease'],
            row.get('remaining_lease_months'),
            row['resale_price']
        ))

    conn.commit()
    cur.close()
    conn.close()
    
    logging.info("[INFO] Inserted all records into hdb_resale_prices.")

    return "hdb_resale_prices"