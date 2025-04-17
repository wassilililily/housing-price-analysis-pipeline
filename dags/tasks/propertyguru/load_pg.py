import pandas as pd
import psycopg2
from airflow.decorators import task
from datetime import datetime

@task
def load_propertyguru_data(listings):
    print(f"[DEBUG] Got {len(listings)} listings")
    
    if not listings:
        print("[ERROR] Listings data is empty!")
        return
    
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS propertyguru_listings (
            id TEXT PRIMARY KEY,
            title TEXT,
            address TEXT,
            price INTEGER,
            bedrooms INTEGER,
            bathrooms INTEGER,
            sqft FLOAT,
            price_psf FLOAT,
            listed_date DATE,
            built INTEGER,
            tenure INTEGER,
            property_type TEXT,
            agent_description TEXT
        );
    """)
    conn.commit()

    inserted_ids = []
            
    for listing in listings:
        print(f"[INSERT] {listing['id']}")
        cur.execute("""
            INSERT INTO propertyguru_listings (
                id, title, address, price, agent_description,
                bedrooms, bathrooms, sqft, price_psf,
                listed_date, built, tenure, property_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            RETURNING id;
        """, (
            listing['id'],
            listing['title'],
            listing['address'],
            listing['price'],
            listing['agent_description'],
            listing.get('bedrooms'),
            listing.get('bathrooms'),
            listing.get('sqft'),
            listing.get('price_psf'),
            listing.get('listed_date'),
            listing.get('built'),
            listing.get('tenure'),
            listing.get('property_type'),
        ))

        try:
            result = cur.fetchone()
            if result:
                inserted_ids.append(result[0])
        except psycopg2.ProgrammingError:
            # No result returned (e.g., due to conflict)
            pass
            
    conn.commit()
    cur.close()
    conn.close()

    return inserted_ids