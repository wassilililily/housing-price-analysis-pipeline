from airflow.decorators import task
import pandas as pd
import psycopg2

@task
def load_monthly_data(monthly_csv):
    df = pd.read_csv(monthly_csv)
    conn = psycopg2.connect(host="postgres", port=5432, dbname="airflow", user="airflow", password="airflow")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS singstat_monthly_data (
            Month TEXT PRIMARY KEY,
            Exchange_Rate FLOAT,
            Interest_Rate FLOAT,
            CPI_2024_base FLOAT
        );
    """)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO singstat_monthly_data (Month, Exchange_Rate, Interest_Rate, CPI_2024_base)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (Month) DO UPDATE SET
                Exchange_Rate = EXCLUDED.Exchange_Rate,
                Interest_Rate = EXCLUDED.Interest_Rate,
                CPI_2024_base = EXCLUDED.CPI_2024_base;
        """, (row["Month"], row["Exchange Rate (SGD/USD)"], row["Interest Rate (%)"], row["CPI (2024 Base)"]))
    conn.commit()
    cur.close()
    conn.close()

@task
def load_quarterly_data(quarterly_csv):
    df = pd.read_csv(quarterly_csv)
    conn = psycopg2.connect(host="postgres", port=5432, dbname="airflow", user="airflow", password="airflow")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS singstat_quarterly_data (
            Year INTEGER,
            Quarter TEXT,
            Unemployment_Rate FLOAT,
            PRIMARY KEY (Year, Quarter)
        );
    """)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO singstat_quarterly_data (Year, Quarter, Unemployment_Rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (Year, Quarter) DO UPDATE SET
                Unemployment_Rate = EXCLUDED.Unemployment_Rate;
        """, (row["Year"], row["Quarter"], row["Unemployment Rate (%)"]))
    conn.commit()
    cur.close()
    conn.close()

@task
def load_annual_basic_data(annual_basic_csv):
    df = pd.read_csv(annual_basic_csv)
    conn = psycopg2.connect(host="postgres", port=5432, dbname="airflow", user="airflow", password="airflow")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS singstat_annual_basic_data (
            Year INTEGER PRIMARY KEY,
            Median_Household_Income FLOAT,
            Median_Individual_Income FLOAT,
            CPI_Change FLOAT
        );
    """)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO singstat_annual_basic_data (Year, Median_Household_Income, Median_Individual_Income, CPI_Change)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (Year) DO UPDATE SET
                Median_Household_Income = EXCLUDED.Median_Household_Income,
                Median_Individual_Income = EXCLUDED.Median_Individual_Income,
                CPI_Change = EXCLUDED.CPI_Change;
        """, (row["Year"], row.get("Median_Household_Income"), row.get("Median_Individual_Income"), row.get("CPI_Change")))
    conn.commit()
    cur.close()
    conn.close()

@task
def load_annual_cpi_categories(annual_cpi_csv):
    df = pd.read_csv(annual_cpi_csv)
    conn = psycopg2.connect(host="postgres", port=5432, dbname="airflow", user="airflow", password="airflow")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS singstat_annual_cpi_categories (
            Year INTEGER,
            Category TEXT,
            CPI_Index FLOAT,
            PRIMARY KEY (Year, Category)
        );
    """)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO singstat_annual_cpi_categories (Year, Category, CPI_Index)
            VALUES (%s, %s, %s)
            ON CONFLICT (Year, Category) DO UPDATE SET
                CPI_Index = EXCLUDED.CPI_Index;
        """, (row["Year"], row["Category"], row["CPI_Index"]))
    conn.commit()
    cur.close()
    conn.close()
