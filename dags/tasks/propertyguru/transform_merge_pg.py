from airflow.decorators import task
import psycopg2
from datetime import datetime
import requests
import json
import time
import pandas as pd
import numpy as np

@task
def transform_merge_propertyguru(listing_ids):
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()

    df = pd.read_csv("/opt/airflow/data/cleaned_housing_data.csv")

    df['storey_numeric'] = df['storey_range']  
    bin_max = int(np.ceil(df['floor_area_sqm'].max() / 10.0)) * 10
    bins = list(range(0, bin_max + 10, 10))
    df['floor_area_bin'] = pd.cut(df['floor_area_sqm'], bins=bins)
    df['floor_area_bin_str'] = df['floor_area_bin'].astype(str)

    mean_storey = (
        df.dropna(subset=['storey_numeric', 'floor_area_bin'])
          .groupby(['district', 'floor_area_bin_str'])['storey_numeric']
          .mean()
          .reset_index()
          .rename(columns={'storey_numeric': 'avg_storey'})
    )

    global_avg_storey = int(round(df['storey_numeric'].mean()))

    create_table_query = """
        CREATE TABLE IF NOT EXISTS propertyguru_data (
            id SERIAL PRIMARY KEY,
            transaction_date DATE,
            district TEXT,
            storey_range FLOAT,
            floor_area_sqm FLOAT,
            lease_commence_year INTEGER,
            remaining_lease_months INTEGER,
            type TEXT,
            price FLOAT,
            price_per_sqm FLOAT,
            exchange_rate FLOAT,
            interest_rate FLOAT,
            cpi FLOAT,
            unemployment_rate FLOAT,
            median_household_income FLOAT,
            median_individual_income FLOAT
        )
    """
    cur.execute(create_table_query)

    insert_data_query = """
        INSERT INTO propertyguru_data (
            transaction_date, district, storey_range, floor_area_sqm,
            lease_commence_year, remaining_lease_months, type, price,
            price_per_sqm, exchange_rate, interest_rate, cpi, unemployment_rate,
            median_household_income, median_individual_income
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    def get_type(property_type):
        property_type = property_type.lower()
        if "hdb" in property_type:
            return "HDB"
        elif "terrace" in property_type:
            return "Terrace"
        elif "executive" in property_type:
            return "Executive"
        elif "condo" in property_type or "apartment" in property_type:
            return "Apartment"
        elif "semi" in property_type:
            return "Semi-Detached"
        elif "bungalow" in property_type:
            return "Detached"
        return None

    month_abbr = {i: m for i, m in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}

    def get_quarter(month):
        return ["1Q", "2Q", "3Q", "4Q"][(month - 1) // 3]

    def sqft_to_sqm(sqft):
        if sqft is None:
            return None
        return sqft * 0.092903

    def calculate_remaining_lease_months(built, tenure, listed_date):
        if not built or not tenure or tenure not in [99, 999]:
            return None
        lease_end_year = built + tenure
        lease_end = datetime(lease_end_year, 1, 1)
        if isinstance(listed_date, str):
            listed_date = datetime.strptime(listed_date, "%Y-%m-%d")
        delta = lease_end.date() - listed_date
        return max(0, delta.days // 30)

    def calc_price_psqm(area, price):
        return round(price / area, 2) if area else None

    def get_singstat_data(cur, year, month):
        year -= 1
        cur.execute("SELECT Exchange_Rate, Interest_Rate, CPI_2024_base FROM singstat_monthly_data WHERE month = %s",
                    (f"{year} {month_abbr[month]}",))
        monthly = cur.fetchone() or (None, None, None)

        cur.execute("SELECT Unemployment_Rate FROM singstat_quarterly_data WHERE year = %s AND quarter = %s",
                    (year, get_quarter(month)))
        quarterly = cur.fetchone() or (None,)

        cur.execute("SELECT Median_Household_Income, Median_Individual_Income FROM singstat_annual_basic_data WHERE year = %s",
                    (year,))
        annual = cur.fetchone() or (None, None)

        return monthly, quarterly, annual

    def postal_code_to_district(postal_code: str) -> str:
        sector = int(postal_code[:2])
        mapping = {
            1:1, 2:1, 3:1, 4:1, 5:1, 6:1, 7:2, 8:2, 14:3, 15:3, 16:3, 9:4, 10:4, 11:5, 12:5, 13:5, 17:6,
            18:7, 19:7, 20:8, 21:8, 22:9, 23:9, 24:10, 25:10, 26:10, 27:10, 28:11, 29:11, 30:11, 31:12,
            32:12, 33:12, 34:13, 35:13, 36:13, 37:13, 38:14, 39:14, 40:14, 41:14, 42:15, 43:15, 44:15,
            45:15, 46:16, 47:16, 48:16, 49:17, 50:17, 81:17, 51:18, 52:18, 53:19, 54:19, 55:19, 82:19,
            56:20, 57:20, 58:21, 59:21, 60:22, 61:22, 62:22, 63:22, 64:22, 65:23, 66:23, 67:23, 68:23,
            69:24, 70:24, 71:24, 72:25, 73:25, 77:26, 78:26, 75:27, 76:27, 79:28, 80:28
        }
        return f"D{mapping.get(sector, 'Unknown')}"

    cur.execute("""
        SELECT id, listed_date, address, sqft, built, tenure, property_type, price, agent_description
        FROM propertyguru_listings
        WHERE id = ANY(%s)
    """, (listing_ids,))
    rows = cur.fetchall()

    for row in rows:
        id, listed_date, address, sqft, built, tenure, property_type, price, _ = row
        monthly, quarterly, annual = get_singstat_data(cur, listed_date.year, listed_date.month)

        try:
            resp = requests.get(
                f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={address}&returnGeom=N&getAddrDetails=Y&pageNum=1")
            addr_info = json.loads(resp.text)["results"][0]
            if addr_info['POSTAL'] == 'NIL':
                continue
            district = postal_code_to_district(addr_info['POSTAL'])
        except:
            continue

        sqm = sqft_to_sqm(sqft)
        if sqm:
            floor_area_bin = pd.cut(pd.Series([sqm]), bins=bins)[0]
            floor_area_bin_str = str(floor_area_bin)
        else:
            floor_area_bin_str = None

        match = mean_storey[
            (mean_storey['district'] == district) &
            (mean_storey['floor_area_bin_str'] == floor_area_bin_str)
        ]

        if match.empty or pd.isna(floor_area_bin_str):
            est_storey = global_avg_storey
        else:
            est_storey = int(round(match['avg_storey'].values[0]))

        merged_row = (
            datetime(listed_date.year, listed_date.month, 1),
            district,
            est_storey,
            sqm,
            built,
            calculate_remaining_lease_months(built, tenure, listed_date),
            get_type(property_type),
            price,
            calc_price_psqm(sqm, price),
            monthly[0], monthly[1], monthly[2],
            quarterly[0],
            annual[0], annual[1]
        )

        cur.execute(insert_data_query, merged_row)
        time.sleep(0.1)

    conn.commit()
    cur.close()
    conn.close()

