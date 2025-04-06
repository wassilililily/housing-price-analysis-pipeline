from airflow.decorators import task
import pandas as pd
import numpy as np
from datetime import datetime
import openpyxl

def csv_file_name(sheet_name):
    return "URA_" + sheet_name.strip().replace(",", "")

def storey_range_to_continuous(range_str):
    if len(range_str) > 1:
        start, end = range_str.split(' to ')  # Split the range
        if start == "B1":
            start, end = -1, -5
        return (int(start) + int(end)) / 2  # Compute midpoint
    return np.nan

def is_price(price):
    try:
        float(price)
        return True
    except (ValueError, TypeError):
        return np.nan
    
def lease_helper(tenure, sale_date):
    if tenure.startswith("999"):
        lease_years = 999
    elif tenure.startswith("99"):
        lease_years = 99
    return lease_years * 12

def calculate_remaining_lease(tenure, sale_date):
    # if not isinstance(tenure, str) or not isinstance(sale_date(datetime, pd.Timestamp)):
    #     return None
    tenure = tenure.strip()
    if tenure.lower() == "freehold":
        return None
    if "from" not in tenure:
        return lease_helper(tenure, sale_date)
    parts = tenure.split("from")
    lease_term = parts[0].strip()
    start_year_str = parts[1].strip()
    if not start_year_str.isdigit():
        return None
    start_year = int(start_year_str)
    if lease_term.startswith("999"):
        lease_years = 999
    elif lease_term.startswith("99"):
        lease_years = 99
    else:
        return None
    # Calculate months
    start_month = 1  # assume January
    months_elapsed = (sale_date.year - start_year) * 12 + (sale_date.month - start_month)
    total_lease_months = lease_years * 12
    remaining_lease_months = total_lease_months - months_elapsed
    return remaining_lease_months if remaining_lease_months >= 0 else 0

@task
def transform_ura(ura_path):
    excel_file = ura_path
    selected_sheets = ["NonStrata", "Strata", "Apt,Condo", "ExCondo"]
    
    for sheet in selected_sheets:
        df = pd.read_excel(excel_file, sheet_name=sheet)
        output_file_name = csv_file_name(sheet)
        df.columns = [
            "project_name", "transacted_price", "area_sqft", "unit_price_psf", "sale_date",
            "street_name", "type_of_sale", "type_of_area", "area_sqm", "unit_price_psm",
            "nett_price", "property_type", "number_of_units", "tenure", "postal_district",
            "market_segment", "floor_level"
        ]
        df['sale_date'] = pd.to_datetime(df['sale_date'], format='%b-%y')
        df['nett_price'] = df['nett_price'].apply(is_price)
        df['type_of_sale'] = df['type_of_sale'].str.strip()
        df['property_type'] = df['property_type'].str.strip()
        df['floor_level_continuous'] = df['floor_level'].apply(storey_range_to_continuous)
        df['remaining_lease_months'] = df.apply(lambda row: calculate_remaining_lease(row['tenure'], row['sale_date']), axis=1)

        output_path = f"/opt/airflow/data/{output_file_name}.csv"
        df.to_csv(output_path, index=False)
        
    return output_path
    
    


