from airflow.decorators import task
import pandas as pd
import psycopg2
import logging

@task
def load_selected_ura_sheets():
    excel_file = 'URA.xlsx'
    selected_sheets = ["NonStrata", "Strata", "Apt,Condo", "ExCondo"]
    conn = psycopg2.connect(host="postgres", port=5432, dbname="airflow", user="airflow", password="airflow")
    cur = conn.cursor()

    def normalize_sheet_name(sheet_name):
        return "URA_" + sheet_name.strip().replace(",", "")
    
    for sheet in selected_sheets:
        table_name = normalize_sheet_name(sheet)
        df = pd.read_excel(excel_file, sheet_name=sheet, usecols="A:Q")

        df.columns = [
            "project_name", "transacted_price", "area_sqft", "unit_price_psf", "sale_date",
            "street_name", "type_of_sale", "type_of_area", "area_sqm", "unit_price_psm",
            "nett_price", "property_type", "number_of_units", "tenure", "postal_district",
            "market_segment", "floor_level"
        ]

        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            project_name TEXT,
            transacted_price NUMERIC,
            area_sqft NUMERIC,
            unit_price_psf NUMERIC,
            sale_date DATE,
            street_name TEXT,
            type_of_sale TEXT,
            type_of_area TEXT,
            area_sqm NUMERIC,
            unit_price_psm NUMERIC,
            nett_price TEXT,
            property_type TEXT,
            number_of_units INTEGER,
            tenure TEXT,
            postal_district INTEGER,
            market_segment TEXT,
            floor_level TEXT
        );
        """
        cur.execute(create_query)
        conn.commit()

        for _, row in df.iterrows():
            values = tuple(row.fillna('').astype(str))
            insert_query = f"""
            INSERT INTO {table_name} (
                project_name, transacted_price, area_sqft, unit_price_psf, sale_date,
                street_name, type_of_sale, type_of_area, area_sqm, unit_price_psm,
                nett_price, property_type, number_of_units, tenure, postal_district,
                market_segment, floor_level
            ) VALUES ({','.join(['%s'] * 17)});
            """
            cur.execute(insert_query, values)

        conn.commit()

    cur.close()
    conn.close()
