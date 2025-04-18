from airflow.decorators import task
import pandas as pd
import psycopg2
import os
import logging

def clean_row(row):
    values = []
    for val in row:
        # Convert NaN or '' to None
        if pd.isna(val) or val == '':
            values.append(None)
        else:
            values.append(val)
    return tuple(values)

@task
def load_ura(ura_csv_paths):
    conn = psycopg2.connect(host="postgres", port=5432, dbname="airflow", user="airflow", password="airflow")
    cur = conn.cursor()

    for ura_csv in ura_csv_paths:
        df = pd.read_csv(ura_csv)
        table_name = os.path.basename(ura_csv).lower().replace(".csv", "")

        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        cur.execute(drop_table_query)
        conn.commit()
        
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                project_name TEXT NOT NULL,
                transacted_price NUMERIC NOT NULL,
                area_sqft NUMERIC NOT NULL,
                unit_price_psf NUMERIC NOT NULL,
                sale_date DATE NOT NULL,
                street_name TEXT,
                type_of_sale TEXT,
                type_of_area TEXT,
                area_sqm NUMERIC,
                unit_price_psm NUMERIC,
                nett_price TEXT,
                property_type TEXT,
                number_of_units INTEGER,
                tenure TEXT,
                postal_district TEXT NOT NULL,
                market_segment TEXT,
                floor_level TEXT,
                floor_level_continuous FLOAT,
                remaining_lease_months INTEGER
            );
        """
        cur.execute(create_query)

        for _, row in df.iterrows():
            # values = (row['project_name'],row['transacted_price'],row['area_sqft'],
            #     row['unit_price_psf'],row['sale_date'],row['street_name'],
            #     row['type_of_sale'],row['type_of_area'],row['area_sqm'],row['unit_price_psm'],
            #     row['nett_price'],row['property_type'],row['number_of_units'],
            #     row['tenure'],row['postal_district'],row['market_segment'],row['floor_level'],
            #     row['floor_level_continuous'],row['remaining_lease_months'])
            insert_query = f"""
                INSERT INTO {table_name} (
                    project_name, transacted_price, area_sqft, unit_price_psf, sale_date,
                    street_name, type_of_sale, type_of_area, area_sqm, unit_price_psm,
                    nett_price, property_type, number_of_units, tenure, postal_district,
                    market_segment, floor_level,floor_level_continuous, remaining_lease_months
                ) VALUES ({','.join(['%s'] * 19)});
                """
            cur.execute(insert_query, clean_row(row))
            logging.info(f"{table_name} created.")
    conn.commit()
    cur.close()
    conn.close()
