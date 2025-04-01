from airflow.decorators import task
import os
import json
import pandas as pd

@task
def process_monthly_data(folder_path_and_files):
    folder_path, downloaded_files = folder_path_and_files
    monthly_file_titles = {
        "Exchange Rates (Average For Period), Monthly": "Exchange Rate (SGD/USD)",
        "Current Banks Interest Rates (End Of Period), Monthly": "Interest Rate (%)",
        "Consumer Price Index (CPI), 2024 As Base Year, Monthly, Seasonally Adjusted": "CPI (2024 Base)"
    }
    dataframes = []

    def parse_json(file_path, column_name):
        with open(file_path, "r") as f:
            raw = json.load(f)
        df = pd.json_normalize(raw["Data"]["row"], record_path="columns")
        df = df[["key", "value"]].rename(columns={"key": "Month", "value": column_name})
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
        return df

    for original_file_name, column_name in monthly_file_titles.items():
        if original_file_name in downloaded_files:
            file_path = os.path.join(folder_path, downloaded_files[original_file_name])
            df = parse_json(file_path, column_name)
            dataframes.append(df)

    if not dataframes:
        return os.path.join(folder_path, "monthly_combined.csv")

    df_combined = dataframes[0]
    for df in dataframes[1:]:
        df_combined = pd.merge(df_combined, df, on="Month", how="outer")

    output_path = os.path.join(folder_path, "monthly_combined.csv")
    df_combined.to_csv(output_path, index=False)
    return output_path

@task
def process_quarterly_data(folder_path_and_files):
    folder_path, downloaded_files = folder_path_and_files
    quarterly_file_titles = {
        "Unemployment Rate (End Of Period), Quarterly, Seasonally Adjusted": "Unemployment Rate (%)"
    }
    dataframes = []

    def parse_quarterly_json(file_path, column_name):
        with open(file_path, "r") as f:
            raw = json.load(f)
        df = pd.json_normalize(raw["Data"].get("row", []), record_path="columns")
        df.columns = ["Quarter", "Value"]
        df[column_name] = pd.to_numeric(df["Value"], errors="coerce")
        df = df.drop(columns=["Value"])
        df["Year"] = df["Quarter"].apply(lambda x: int(x.split(" ")[0]))
        df["Quarter"] = df["Quarter"].apply(lambda x: x.split(" ")[1])
        return df

    for original_file_name, column_name in quarterly_file_titles.items():
        if original_file_name in downloaded_files:
            file_path = os.path.join(folder_path, downloaded_files[original_file_name])
            df = parse_quarterly_json(file_path, column_name)
            dataframes.append(df)

    if not dataframes:
        return os.path.join(folder_path, "quarterly_combined.csv")

    df_combined = dataframes[0]
    for df in dataframes[1:]:
        df_combined = pd.merge(df_combined, df, on=["Year", "Quarter"], how="outer")

    output_path = os.path.join(folder_path, "quarterly_combined.csv")
    df_combined.to_csv(output_path, index=False)
    return output_path

@task
def process_annual_basic_data(folder_path_and_files):
    folder_path, downloaded_files = folder_path_and_files
    file_titles = {
        "Key Indicators On Household Employment Income Among Resident Employed Households, Annual": "Median_Household_Income",
        "Median Gross Monthly Income From Employment (Including Employer CPF) Of Full-Time Employed Residents By Occupations And Sex, End June, Annual": "Median_Individual_Income",
        "Percent Change In Consumer Price Index (CPI) Over Corresponding Period Of Previous Year, 2024 As Base Year, Annual": "CPI_Change"
    }
    dataframes = []

    def parse_annual(file_path, value_name):
        with open(file_path, "r") as f:
            raw = json.load(f)
        extracted_data = []
        for row in raw["Data"].get("row", []):
            for col in row.get("columns", []):
                try:
                    year = int(col.get("key", "0"))
                    value = float(col.get("value", "0"))
                    extracted_data.append({"Year": year, value_name: value})
                except:
                    continue
        return pd.DataFrame(extracted_data)

    for title, column_name in file_titles.items():
        if title in downloaded_files:
            file_path = os.path.join(folder_path, downloaded_files[title])
            df = parse_annual(file_path, column_name)
            if not df.empty:
                dataframes.append(df)

    if not dataframes:
        return os.path.join(folder_path, "annual_basic_combined.csv")

    df_combined = dataframes[0]
    for df in dataframes[1:]:
        df_combined = pd.merge(df_combined, df, on="Year", how="outer")
    df_combined.sort_values("Year", inplace=True)
    df_combined.interpolate(method="linear", inplace=True)

    output_path = os.path.join(folder_path, "annual_basic_combined.csv")
    df_combined.to_csv(output_path, index=False)
    return output_path

@task
def process_annual_cpi_categories(folder_path_and_files):
    folder_path, downloaded_files = folder_path_and_files
    file_title = "Consumer Price Index (CPI) By Household Income Group, Middle 60%, 2019 As Base Year, Annual"
    if file_title not in downloaded_files:
        return os.path.join(folder_path, "annual_cpi_categories.csv")

    file_path = os.path.join(folder_path, downloaded_files[file_title])
    with open(file_path, "r") as f:
        raw = json.load(f)

    records = []
    for row in raw["Data"].get("row", []):
        category = row.get("rowText", "Unknown")
        for col in row.get("columns", []):
            try:
                year = int(col.get("key"))
                value = float(col.get("value"))
                records.append({"Year": year, "Category": category, "CPI_Index": value})
            except:
                continue

    df = pd.DataFrame(records)
    df.sort_values(["Category", "Year"], inplace=True)
    df["CPI_Index"] = df.groupby("Category")["CPI_Index"].transform(lambda x: x.interpolate(limit_direction="both"))

    output_path = os.path.join(folder_path, "annual_cpi_categories.csv")
    df.to_csv(output_path, index=False)
    return output_path