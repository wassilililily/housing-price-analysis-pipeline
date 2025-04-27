# Singapore Housing Price Analysis – ETLT Pipeline (Airflow)

This project uses Apache Airflow to build a modular ETLT pipeline for analyzing housing price trends in Singapore using data from HDB, URA, economic indicators, and PropertyGuru.

---

## Getting Started (Not Recommended)

For most users, please use Docker Compose to setup the project. View "Running with Docker Compose" section below.

### 1. **Clone the repo**
```bash
git clone https://github.com/JingLeEa/housing-price-analysis-pipeline.git
cd housing-price-analysis-pipeline
```

### 2. **Install dependencies (if running locally)**
```bash
pip install -r requirements.txt
```

> If you're using **Docker (recommended)**, dependencies are handled in the Dockerfile.

---

## Running with Docker Compose

### 1. **Build and start the containers**
```bash
docker compose build
docker compose up airflow-init
docker compose up
```

### 2. **Access the Airflow UI**
Access airflow at [http://localhost:8080](http://localhost:8080).

Default credentials:  
- Username: `airflow`  
- Password: `airflow`

---

## Available DAGs
### `housing_sg__etlt_pipeline__weekly`
- **Purpose**: Weekly batch ETLT pipeline for housing transactions and economic indicators.
- **Tasks**:
  - **Extract**:
    - HDB Resale Price API
    - URA Private Property Transactions (manual Excel sheets)
    - SingStat Economic Indicators API
  - **Transform**: Cleaning, feature engineering, and staging into the data warehouse.
  - **Load**: Insert cleaned data into staging tables.
  - **Final Transformation**: Merge HDB, URA, and SingStat data into the `housing_data` mart for downstream analytics.

---

### `propertyguru__etlt__2h`
- **Purpose**: Scrapes the latest PropertyGuru listings every 2 hours.
- **Tasks**:
  - **Extract**: Scrape up to 20 most recent listings from PropertyGuru using Selenium.
  - **Transform**: Parse, clean, and enrich property data (e.g., geocoding postal codes to districts).
  - **Load**: Insert cleaned listings into the `propertyguru_data` table.

---

### `housing_sg__ml_training__monthly`
- **Purpose**: Monthly retraining of machine learning models on updated housing datasets.
- **Tasks**:
  - Load datasets from the data mart.
  - Preprocess features, including handling 1-year lagged economic indicators.
  - Train regression models (LightGBM, Random Forest, ElasticNet) and output evaluation metrics.

---

### `housing_visualisation__monthly`
- **Purpose**: (Optional) Manually triggered DAG for generating updated dashboard reports.
- **Tasks**:
  - Read from the `housing_data` table.
  - Generate multiple visualizations, such as price trends, correlation heatmaps, district price comparisons, and more.
  - Compile all generated charts into a single PDF report for easy review.

---

## References

- [HDB Data](https://data.gov.sg/dataset/resale-flat-prices)
- [URA Transactions](https://www.ura.gov.sg/realEstateIIWeb/transaction/search.action)
- [SingStat APIs](https://tablebuilder.singstat.gov.sg)
- [Property Guru](https://www.propertyguru.com.sg)
- [Apache Airflow Docs](https://airflow.apache.org/docs)

---

## Team 16 – IS3107
- Ea Jing Le
- Han Seungju
- Kong Hoi Tec
- Mau Ze Wei
- Shin Jisu
