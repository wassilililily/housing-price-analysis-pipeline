import os
import json
import pandas as pd
import requests
from urllib.parse import quote
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from bs4 import BeautifulSoup

DEFAULT_ARGS = {"owner": "airflow", "start_date": datetime(2025, 3, 29)}

with DAG(dag_id="singstat_data_processing", default_args=DEFAULT_ARGS, schedule_interval="@daily", catchup=False) as dag:

    @task
    def search_files():
        desired_files = [
            "Exchange Rates (Average For Period), Monthly",
            "Key Indicators On Household Employment Income Among Resident Employed Households, Annual",
            "Median Gross Monthly Income From Employment (Including Employer CPF) Of Full-Time Employed Residents By Occupations And Sex, End June, Annual",
            "Unemployment Rate (End Of Period), Quarterly, Seasonally Adjusted",
            "Current Banks Interest Rates (End Of Period), Monthly",
            "Consumer Price Index (CPI), 2024 As Base Year, Monthly, Seasonally Adjusted",
            "Percent Change In Consumer Price Index (CPI) Over Corresponding Period Of Previous Year, 2024 As Base Year, Annual",
            "Consumer Price Index (CPI) By Household Income Group, Middle 60%, 2019 As Base Year, Annual"
        ]
        available_files = []
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        for file_name in desired_files:
            encoded_title = quote(file_name)
            search_url = f"https://tablebuilder.singstat.gov.sg/api/table/resourceid?keyword={encoded_title}&searchOption=all"
            response = requests.get(search_url, headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch search results for '{file_name}': Status {response.status_code}")
                continue
            data = response.json()
            print(f"API Response for '{file_name}': {json.dumps(data, ensure_ascii=False, indent=2)}")
            if data.get("StatusCode") == 200 and data.get("Data") and data["Data"].get("records"):
                records = data["Data"]["records"]
                for record in records:
                    response_title = record.get("title", "").strip()
                    if file_name.lower() == response_title.lower():
                        available_files.append({"title": response_title, "id": record["id"]})
                        print(f"Found exact match: {response_title} (ID: {record['id']})")
                    elif file_name.lower() in response_title.lower():
                        available_files.append({"title": response_title, "id": record["id"]})
                        print(f"Found partial match: {response_title} (ID: {record['id']})")
        if not available_files:
            print("no available files")
        else:
            print(f"total number of file: {len(available_files)}")
        return available_files

    @task
    def download_files(file_info_list: list):
        folder_path = "/opt/airflow/project"
        os.makedirs(folder_path, exist_ok=True)
        downloaded_files = {}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        for file_info in file_info_list:
            file_name = file_info["title"]
            resource_id = file_info["id"]
            sanitized_file_name = file_name.replace(" ", "_")
            download_url = f"https://tablebuilder.singstat.gov.sg/api/table/tabledata/{resource_id}"
            save_path = os.path.join(folder_path, f"{sanitized_file_name}.json")
            response = requests.get(download_url, headers=headers)
            if response.status_code == 200:
                with open(save_path, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {file_name} -> {save_path}")
                downloaded_files[file_name] = sanitized_file_name + ".json"
            else:
                print(f"Failed to download {file_name}: Status {response.status_code}")
        return folder_path, downloaded_files

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
            print(f"Opening file: {file_path}")
            with open(file_path, "r") as f:
                raw = json.load(f)
            if "Data" in raw and "row" in raw["Data"]:
                df = pd.json_normalize(raw["Data"]["row"], record_path="columns")
            else:
                df = pd.DataFrame(raw)
            print("Dataframe columns before filtering:", df.columns.tolist())
            if "key" in df.columns and "value" in df.columns:
                df = df[["key", "value"]]
                df.columns = ["Month", "Value"]
            else:
                print("Expected columns 'key' and 'value' not found.")
            df[column_name] = pd.to_numeric(df["Value"], errors="coerce")
            df = df.drop(columns=["Value"])
            return df
        for original_file_name, column_name in monthly_file_titles.items():
            if original_file_name in downloaded_files:
                file_name = downloaded_files[original_file_name]
                file_path = os.path.join(folder_path, file_name)
                print(f"Processing file: {file_path}")
                df = parse_json(file_path, column_name)
                dataframes.append(df)
            else:
                print(f"Warning: File not found - {original_file_name}")
        if not dataframes:
            print("No monthly data files were processed")
            monthly_output = os.path.join(folder_path, "monthly_combined.csv")
            pd.DataFrame().to_csv(monthly_output, index=False)
            return monthly_output
        df_combined = dataframes[0]
        for df in dataframes[1:]:
            df_combined = pd.merge(df_combined, df, on="Month", how="outer")
        monthly_output = os.path.join(folder_path, "monthly_combined.csv")
        df_combined.to_csv(monthly_output, index=False)
        print("Monthly data processed and saved to", monthly_output)
        return monthly_output

    @task
    def process_quarterly_data(folder_path_and_files):
        folder_path, downloaded_files = folder_path_and_files
        quarterly_file_titles = {
            "Unemployment Rate (End Of Period), Quarterly, Seasonally Adjusted": "Unemployment Rate (%)"
        }
        dataframes = []
        def parse_quarterly_json(file_path, column_name):
            print(f"Opening file: {file_path}")
            try:
                with open(file_path, "r") as f:
                    raw = json.load(f)
                if "Data" in raw and "row" in raw["Data"]:
                    data = raw["Data"]
                else:
                    data = raw
                df = pd.json_normalize(data.get("row", []), record_path="columns")
                df.columns = ["Quarter", "Value"]
                df[column_name] = pd.to_numeric(df["Value"], errors="coerce")
                df = df.drop(columns=["Value"])
                df["Year"] = df["Quarter"].apply(lambda x: int(x.split(" ")[0]))
                df["Quarter"] = df["Quarter"].apply(lambda x: x.split(" ")[1])
                return df
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
                return pd.DataFrame(columns=["Year", "Quarter", column_name])
        for original_file_name, column_name in quarterly_file_titles.items():
            if original_file_name in downloaded_files:
                file_name = downloaded_files[original_file_name]
                file_path = os.path.join(folder_path, file_name)
                print(f"Processing file: {file_path}")
                df = parse_quarterly_json(file_path, column_name)
                dataframes.append(df)
            else:
                print(f"Warning: File not found - {original_file_name}")
        if not dataframes:
            print("No quarterly data files were processed")
            quarterly_output = os.path.join(folder_path, "quarterly_combined.csv")
            pd.DataFrame(columns=["Year", "Quarter"]).to_csv(quarterly_output, index=False)
            return quarterly_output
        df_quarterly_combined = dataframes[0]
        for df in dataframes[1:]:
            df_quarterly_combined = pd.merge(df_quarterly_combined, df, on=["Year", "Quarter"], how="outer")
        quarterly_output = os.path.join(folder_path, "quarterly_combined.csv")
        df_quarterly_combined.to_csv(quarterly_output, index=False)
        print("Quarterly data processed and saved to", quarterly_output)
        return quarterly_output

    @task
    def process_annual_basic_data(folder_path_and_files):
        folder_path, downloaded_files = folder_path_and_files
        file_titles = {
            "Key Indicators On Household Employment Income Among Resident Employed Households, Annual": "household_income",
            "Median Gross Monthly Income From Employment (Including Employer CPF) Of Full-Time Employed Residents By Occupations And Sex, End June, Annual": "individual_income",
            "Percent Change In Consumer Price Index (CPI) Over Corresponding Period Of Previous Year, 2024 As Base Year, Annual": "cpi_change"
        }
        def fast_extract_json(file_path, value_name):
            print(f"Opening file: {file_path}")
            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    raw = json.load(file)
                if "Data" in raw and "row" in raw["Data"]:
                    data = raw["Data"]
                else:
                    data = raw
                extracted_data = []
                for row in data.get("row", []):
                    for col in row.get("columns", []):
                        try:
                            year_str = col.get("key", "0")
                            value_str = col.get("value", "0")
                            if not year_str:
                                continue
                            year = int(year_str)
                            value = float(value_str)
                            extracted_data.append({"Year": year, value_name: value})
                        except (ValueError, TypeError) as e:
                            print(f"Error parsing data point in {file_path}: {str(e)}")
                df = pd.DataFrame(extracted_data)
                return df
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
                return pd.DataFrame(columns=["Year", value_name])
        dfs = []
        for original_file_name, key in file_titles.items():
            if original_file_name in downloaded_files:
                file_name = downloaded_files[original_file_name]
                file_path = os.path.join(folder_path, file_name)
                if key == "household_income":
                    value_name = "Median_Household_Income"
                elif key == "individual_income":
                    value_name = "Median_Individual_Income"
                elif key == "cpi_change":
                    value_name = "CPI_Change"
                print(f"Processing file: {file_path}")
                df = fast_extract_json(file_path, value_name)
                if "Year" not in df.columns or df.empty:
                    print(f"Dataframe from {original_file_name} does not contain 'Year' data, skipping.")
                    continue
                df.drop_duplicates(subset="Year", keep="first", inplace=True)
                dfs.append(df)
            else:
                print(f"Warning: File not found - {original_file_name}")
        if not dfs:
            print("No annual basic data files were processed")
            annual_basic_output = os.path.join(folder_path, "annual_basic_combined.csv")
            pd.DataFrame(columns=["Year"]).to_csv(annual_basic_output, index=False)
            return annual_basic_output
        merged_data = dfs[0]
        for df in dfs[1:]:
            if "Year" in df.columns:
                merged_data = merged_data.merge(df, on="Year", how="outer")
            else:
                print("Skipping a dataframe without 'Year' column during merge.")
        merged_data.sort_values(by="Year", inplace=True)
        merged_data.interpolate(method="linear", inplace=True)
        annual_basic_output = os.path.join(folder_path, "annual_basic_combined.csv")
        merged_data.to_csv(annual_basic_output, index=False)
        print("Annual basic data processed and saved to", annual_basic_output)
        return annual_basic_output

    @task
    def process_annual_cpi_categories(folder_path_and_files):
        folder_path, downloaded_files = folder_path_and_files
        cpi_file_title = "Consumer Price Index (CPI) By Household Income Group, Middle 60%, 2019 As Base Year, Annual"
        if cpi_file_title not in downloaded_files:
            print(f"Warning: File not found - {cpi_file_title}")
            annual_cpi_output = os.path.join(folder_path, "annual_cpi_categories.csv")
            pd.DataFrame(columns=["Year", "Category", "CPI_Index"]).to_csv(annual_cpi_output, index=False)
            return annual_cpi_output
        file_name = downloaded_files[cpi_file_title]
        file_path = os.path.join(folder_path, file_name)
        print(f"Opening file: {file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                raw = json.load(file)
            if "Data" in raw and "row" in raw["Data"]:
                data = raw["Data"]
            else:
                data = raw
            def extract_cpi_categories(data):
                cpi_records = []
                for category in data.get("row", []):
                    category_name = category.get("rowText", "Unknown")
                    for entry in category.get("columns", []):
                        try:
                            year = int(entry.get("key", "0"))
                            value = float(entry.get("value", "0"))
                            cpi_records.append({"Year": year, "Category": category_name, "CPI_Index": value})
                        except (ValueError, TypeError) as e:
                            print(f"Error parsing CPI data point: {str(e)}")
                return pd.DataFrame(cpi_records)
            cpi_df = extract_cpi_categories(data)
            if cpi_df.empty:
                print("No CPI category data was extracted")
                annual_cpi_output = os.path.join(folder_path, "annual_cpi_categories.csv")
                pd.DataFrame(columns=["Year", "Category", "CPI_Index"]).to_csv(annual_cpi_output, index=False)
                return annual_cpi_output
            cpi_df.sort_values(["Category", "Year"], inplace=True)
            cpi_df["Year"] = cpi_df["Year"].astype(int)
            if cpi_df["CPI_Index"].isnull().any():
                cpi_df["CPI_Index"] = (
                    cpi_df.groupby("Category")["CPI_Index"]
                    .apply(lambda x: x.interpolate(method="linear", limit_direction="both"))
                    .reset_index(level=0, drop=True)
                )
            annual_cpi_output = os.path.join(folder_path, "annual_cpi_categories.csv")
            cpi_df.to_csv(annual_cpi_output, index=False)
            print("Annual CPI categories data processed and saved to", annual_cpi_output)
            return annual_cpi_output
        except Exception as e:
            print(f"Error processing annual CPI categories: {str(e)}")
            annual_cpi_output = os.path.join(folder_path, "annual_cpi_categories.csv")
            pd.DataFrame(columns=["Year", "Category", "CPI_Index"]).to_csv(annual_cpi_output, index=False)
            return annual_cpi_output

    file_names = search_files()
    folder_path_and_files = download_files(file_names)
    monthly_output = process_monthly_data(folder_path_and_files)
    quarterly_output = process_quarterly_data(folder_path_and_files)
    annual_basic_output = process_annual_basic_data(folder_path_and_files)
    annual_cpi_output = process_annual_cpi_categories(folder_path_and_files)