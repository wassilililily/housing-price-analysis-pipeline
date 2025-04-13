from airflow.decorators import task
import psycopg2
from datetime import datetime

@task
def transform_merge_data():
  conn = psycopg2.connect(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="postgres",
    port=5432
  )
  cur = conn.cursor()

  create_table_query = """
    CREATE TABLE IF NOT EXISTS housing_data (
      id SERIAL PRIMARY KEY,
      transaction_date DATE,
      town TEXT,
      flat_type TEXT,
      storey_range FLOAT,
      floor_area_sqm FLOAT,
      lease_commence_year INTEGER,
      remaining_lease_months INTEGER,
      type TEXT,
      price FLOAT,
      exchange_rate FLOAT,
      interest_rate FLOAT,
      cpi FLOAT,
      unemployment_rate FLOAT,
      median_household_income FLOAT,
      median_individual_income FLOAT
    ) """
  cur.execute(create_table_query)

  insert_data_query = """
    INSERT INTO housing_data (
      transaction_date, town, flat_type, storey_range, floor_area_sqm,
      lease_commence_year, remaining_lease_months, type, price,
      exchange_rate, interest_rate, cpi, unemployment_rate,
      median_household_income, median_individual_income
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
  """

  # Helper functions
  month_abbr = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
              5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
              9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}

  def get_quarter(month):
    if month in (1, 2, 3):
        return "1Q"
    elif month in (4, 5, 6):
        return "2Q"
    elif month in (7, 8, 9):
        return "3Q"
    elif month in (10, 11, 12):
        return "4Q"
  
  def get_singstat_data(cur, year, month):
    """
    Extract SingStat data (monthly, quarterly, and annual) for the given year and month.

    Args:
        cur: Database cursor object.
        year: Year as an integer.
        month: Month as an integer.

    Returns:
        A tuple containing:
        - Monthly data (Exchange_Rate, Interest_Rate, CPI_2024_base)
        - Quarterly data (Unemployment_Rate)
        - Annual data (Median_Household_Income, Median_Individual_Income)
    """
    # Monthly data
    singstat_month_str = f"{year} {month_abbr[int(month)]}"
    singstat_monthly_data_sql_chunk = """
      SELECT Exchange_Rate, Interest_Rate, CPI_2024_base
      FROM singstat_monthly_data
      WHERE month = %s
    """
    cur.execute(singstat_monthly_data_sql_chunk, (singstat_month_str,))
    singstat_monthly_data_chunk = cur.fetchone() or (None, None, None)

    # Quarterly data
    quarter_str = get_quarter(int(month))
    singstat_quarterly_data_sql_chunk = """
      SELECT Unemployment_Rate
      FROM singstat_quarterly_data
      WHERE year = %s
        AND quarter = %s
    """
    cur.execute(singstat_quarterly_data_sql_chunk, (year, quarter_str))
    singstat_quarterly_data_chunk = cur.fetchone() or (None,)

    # Annual data
    singstat_annual_data_sql_chunk = """
      SELECT Median_Household_Income, Median_Individual_Income
      FROM singstat_annual_basic_data
      WHERE year = %s
    """
    cur.execute(singstat_annual_data_sql_chunk, (year,))
    singstat_annual_data_chunk = cur.fetchone() or (None, None)

    return singstat_monthly_data_chunk, singstat_quarterly_data_chunk, singstat_annual_data_chunk

  # HDB resale database
  distinct_months_query_hdb_resale = """
    SELECT DISTINCT
      EXTRACT(YEAR FROM month) AS year,
      EXTRACT(MONTH FROM month) AS month
    FROM hdb_resale_prices
    ORDER BY year, month;
    """

  cur.execute(distinct_months_query_hdb_resale)
  rows = cur.fetchall()
  distinct_year_months_hdb_resale = [(int(r[0]), int(r[1])) for r in rows]

  for year, month in distinct_year_months_hdb_resale:
    hdb_sql_chunk = """
      SELECT town, flat_type, storey_range_continuous, floor_area_sqm, lease_commence_date, remaining_lease_months, resale_price
      FROM hdb_resale_prices
      WHERE EXTRACT(YEAR FROM month) = %s
        AND EXTRACT(MONTH FROM month) = %s
    """
    cur.execute(hdb_sql_chunk, (year, month))
    hdb_data_chunk = cur.fetchall()
    singstat_monthly_data_chunk, singstat_quarterly_data_chunk, singstat_annual_data_chunk = get_singstat_data(cur, year, month)

    for row in hdb_data_chunk:
      (town, flat_type, storey_range, floor_area_sqm, lease_commence_date, remaining_lease_months, resale_price) = row

      merged_row = (
          datetime(year, month, 1),
          town,
          flat_type,
          storey_range,
          floor_area_sqm,
          lease_commence_date,
          remaining_lease_months,
          "HDB",
          resale_price,
          singstat_monthly_data_chunk[0],
          singstat_monthly_data_chunk[1],
          singstat_monthly_data_chunk[2],
          singstat_quarterly_data_chunk[0],
          singstat_annual_data_chunk[0],
          singstat_annual_data_chunk[1]
        )
      cur.execute(insert_data_query, merged_row)
    conn.commit()

  # URA database
  distinct_months_query_ura = """
    SELECT DISTINCT
      EXTRACT(YEAR FROM sale_date) AS year,
      EXTRACT(MONTH FROM sale_date) AS month
    FROM ura_excondo
    ORDER BY year, month;
  """

  cur.execute(distinct_months_query_ura)
  row = cur.fetchall()
  distinct_year_months_ura = [(int(r[0]), int(r[1])) for r in row]

  for year, month in distinct_year_months_ura:
    ura_sql_chunk = """
    SELECT street_name, property_type, floor_level_continuous, area_sqm, tenure, remaining_lease_months, transacted_price
      FROM ura_excondo
      WHERE EXTRACT(YEAR FROM sale_date) = %s
        AND EXTRACT(MONTH FROM sale_date) = %s
    """
    cur.execute(ura_sql_chunk, (year, month))
    ura_data_chunk = cur.fetchall()
    singstat_monthly_data_chunk, singstat_quarterly_data_chunk, singstat_annual_data_chunk = get_singstat_data(cur, year, month)

    for row in ura_data_chunk:
      (town, flat_type, storey_range, floor_area_sqm, tenure, remaining_lease_months, resale_price) = row
      try:
          lease_commence_date = int(tenure.strip().split(" ")[-1])
      except ValueError:
          lease_commence_date = None

      merged_row = (
          datetime(year, month, 1),
          town,
          flat_type,
          storey_range,
          floor_area_sqm,
          lease_commence_date,
          remaining_lease_months,
          "URA",
          resale_price,
          singstat_monthly_data_chunk[0],
          singstat_monthly_data_chunk[1],
          singstat_monthly_data_chunk[2],
          singstat_quarterly_data_chunk[0],
          singstat_annual_data_chunk[0],
          singstat_annual_data_chunk[1]
        )
      cur.execute(insert_data_query, merged_row)
    conn.commit()

  cur.close()
  conn.close()
