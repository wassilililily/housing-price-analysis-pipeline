from airflow.decorators import task
import pandas as pd
import numpy as np
import logging

def convert_to_months(lease):
    if pd.isna(lease):  # Handle NaN values
        return np.nan
    elif isinstance(lease, str):
        parts = lease.split()
        years = int(parts[0])  # First value is always years
        months = int(parts[2]) if "months" in lease else 0  # Check for months
        return (years * 12) + months
    else:
        return lease  # Already numeric (if any)
    
def storey_range_to_continuous(range_str):
    if isinstance(range_str, str):  # Ensure the value is a string
        start, end = range_str.split(' TO ')  # Split the range
        return (int(start) + int(end)) / 2  # Compute midpoint
    return np.nan

@task
def transform_hdb(file_path):
    df = pd.read_csv(file_path, low_memory=False)

    #Convert storey range to continuous variable
    df['storey_range_continuous'] = df['storey_range'].apply(storey_range_to_continuous)

    #Convert lease remaining from years and months to months
    df['remaining_lease_months'] = df['remaining_lease'].apply(convert_to_months)

    #Fill up empty values with lease remaining from point of sale
    df['month'] = pd.to_datetime(df['month'], format='%Y-%m')
    df['transaction_year'] = df['month'].dt.year
    df['computed_remaining_lease_months'] = (99 * 12) - ((df['transaction_year'] - df['lease_commence_date']) * 12)
    df['remaining_lease_months'].fillna(df['computed_remaining_lease_months'], inplace=True)

    df.drop(columns=['computed_remaining_lease_months', 'transaction_year'], inplace=True)

    #Standardised flat_type and flat_model values
    df['flat_type'] = df['flat_type'].str.strip()
    df['flat_model'] = df['flat_model'].str.strip().str.lower()

    output_file = "/opt/airflow/data/hdb_resale_transformed.csv"
    df.to_csv(output_file, index=False)
    logging.info(f"Transformed data saved to {output_file}")

    return output_file