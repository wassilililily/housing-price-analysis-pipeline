from airflow.decorators import task
import os
import json
import requests
from urllib.parse import quote

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
    headers = {"User-Agent": "Mozilla/5.0"}
    for file_name in desired_files:
        encoded_title = quote(file_name)
        search_url = f"https://tablebuilder.singstat.gov.sg/api/table/resourceid?keyword={encoded_title}&searchOption=all"
        response = requests.get(search_url, headers=headers)
        if response.status_code != 200:
            continue
        data = response.json()
        if data.get("StatusCode") == 200 and data.get("Data") and data["Data"].get("records"):
            records = data["Data"]["records"]
            for record in records:
                response_title = record.get("title", "").strip()
                if file_name.lower() in response_title.lower():
                    available_files.append({"title": response_title, "id": record["id"]})
    return available_files

@task
def download_files(file_info_list: list):
    folder_path = "/opt/airflow/data"
    os.makedirs(folder_path, exist_ok=True)
    downloaded_files = {}
    headers = {"User-Agent": "Mozilla/5.0"}
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
            downloaded_files[file_name] = sanitized_file_name + ".json"
    return folder_path, downloaded_files