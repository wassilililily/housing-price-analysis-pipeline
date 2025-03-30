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
  conn.commit(create_table_query)

  distinct_months_query = """
    SELECT DISTINCT
      EXTRACT(YEAR FROM transaction_date) AS year,
      EXTRACT(MONTH FROM transaction_date) AS month
    FROM hdb_resale_prices
    ORDER BY year, month;
    """

  cur.execute(distinct_months_query)
  rows = cur.fetchall()
  distinct_year_months = [(int(r[0]), int(r[1])) for r in rows]

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

  for year, month in distinct_year_months:
    hdb_sql_chunk = """
      SELECT town, flat_type, storey_range, floor_area_sqm, lease_commence_date, remaining_lease_months, resale_price
      FROM hdb_resale_prices
      WHERE EXTRACT(YEAR FROM transaction_date) = %s
        AND EXTRACT(MONTH FROM transaction_date) = %s
    """
    cur.execute(hdb_sql_chunk, (year, month))
    hdb_data_chunk = cur.fetchall()

    singstat_month_str = f"{year} {month_abbr[int(month)]}"
    singstat_monthly_data_sql_chunk = """
      SELECT Exchange_Rate, Interest_Rate, CPI_2024_base
      FROM singstat_monthly_data
      WHERE month = %s
    """
    cur.execute(singstat_monthly_data_sql_chunk, (singstat_month_str,))
    singstat_monthly_data_chunk = cur.fetchone()

    quarter_str = get_quarter(int(month))
    singstat_quarterly_data_sql_chunk = """
      SELECT Unemployment_Rate
      FROM singstat_quarterly_data
      WHERE year = %s
        AND quarter = %s
    """
    cur.execute(singstat_quarterly_data_sql_chunk, (year, quarter_str))
    singstat_quarterly_data_chunk = cur.fetchone()

    singstat_annual_data_sql_chunk = """
      SELECT Median_Household_Income, Median_Individual_Income
      FROM singstat_annual_basic_data
      WHERE year = %s
    """
    cur.execute(singstat_annual_data_sql_chunk, (year, ))
    singstat_annual_data_chunk = cur.fetchone()

    # Handle None values
    if not monthly_data:
        monthly_data = (None, None, None)
    if not quarterly_data:
        quarterly_data = (None,)
    if not annual_data:
        annual_data = (None, None)


    insert_sql = """
      INSERT INTO housing_data (
        transaction_date, town, flat_type, storey_range, floor_area_sqm,
        lease_commence_year, remaining_lease_months, type, price,
        exchange_rate, interest_rate, cpi, unemployment_rate,
        median_household_income, median_individual_income
      )
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

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
      cur.execute(insert_sql, merged_row)
    conn.commit()

  cur.close()
  conn.close()
