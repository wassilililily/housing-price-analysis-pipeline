# Singapore Housing Price Analysis – ETL Pipeline (Airflow)

This project uses Apache Airflow to build a modular ETL pipeline for analyzing housing price trends in Singapore using data from HDB, URA, economic indicators, and PropertyGuru.

---

## Getting Started

### 1. **Clone the repo**
```bash
git clone https://github.com/JingLeEa/housing-price-analysis-pipeline.git
cd housing-price-analysis-pipeline
```

### 2. **Install dependencies (if running locally)**
```bash
pip install -r requirements.txt
```

> If you're using Docker (recommended), dependencies are handled in the Dockerfile.

---

## Running with Docker Compose

### 1. **Build and start the containers**
```bash
docker-compose up airflow-init
docker-compose up
```

### 2. **Access the Airflow UI**
[http://localhost:8080](http://localhost:8080)  
Default credentials:  
- Username: `airflow`  
- Password: `airflow`

---

## DAG: `housing_sg__etl_pipeline__weekly`

This DAG does the following:

1. **Extracts** data from:
   - HDB Resale dataset
   - URA Private Property transactions
   - SingStat economic indicators
   - PropertyGuru (web scraping)

2. **Transforms** and merges the datasets

3. **Loads** the final dataset into a database or file

Each task is modularized under `dags/tasks/` so that different team members can work independently.

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
