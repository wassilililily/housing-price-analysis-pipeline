from airflow.decorators import task
import psycopg2
from datetime import datetime

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

  create_table_query = """
    CREATE TABLE IF NOT EXISTS propertyguru_data (
      id SERIAL PRIMARY KEY,
      transaction_date DATE,
      town TEXT,
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
    ) """
  cur.execute(create_table_query)

  insert_data_query = """
    INSERT INTO propertyguru_data (
      transaction_date, town, storey_range, floor_area_sqm,
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
    else:
      return None

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

  def sqft_to_sqm(sqft):
    return sqft * 0.092903

  def calculate_remaining_lease_months(built, tenure, listed_date):
    if not built or not tenure or tenure not in [99, 999]:
      return None

    lease_end_year = built + tenure
    lease_end = datetime(lease_end_year, 1, 1)
    
    # Ensure listed_date is datetime
    if isinstance(listed_date, str):
      listed_date = datetime.strptime(listed_date, "%Y-%m-%d")
    
    delta = lease_end.date() - listed_date
    return max(0, delta.days // 30)  # Approximate to months

  def calc_price_psqm(area, price):
    return round(price/area, 2)

  def get_singstat_data(cur, year, month):
    """
    Extract SingStat data (monthly, quarterly, and annual) for the given (year - 1) and month.

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
    time_lag = 1 # Time lag in years
    year = year - time_lag

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
  
  property_guru_sql = """
    SELECT 
      id,
      listed_date,
      address,
      sqft,
      built,
      tenure,
      property_type,
      price,
      agent_description
    FROM propertyguru_listings
    WHERE id = ANY(%s)
  """

  cur.execute(property_guru_sql, (listing_ids,))
  rows = cur.fetchall()

  for row in rows:
    (id, listed_date, address, sqft, built, tenure, property_type, price, agent_description) = row
    singstat_monthly_data_chunk, singstat_quarterly_data_chunk, singstat_annual_data_chunk = get_singstat_data(cur, listed_date.year, listed_date.month)

    merged_row = (
      datetime(listed_date.year, listed_date.month, 1),
      address,
      None,
      sqft_to_sqm(sqft),
      built,
      calculate_remaining_lease_months(built, tenure, listed_date),
      get_type(property_type),
      price,
      calc_price_psqm(sqft_to_sqm(sqft), price),
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
