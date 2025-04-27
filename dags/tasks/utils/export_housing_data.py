from airflow.decorators import task
import psycopg2

@task()
def export_data():
  conn = psycopg2.connect(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="postgres",
    port=5432
  )
  cur = conn.cursor()

  with conn:
    with conn.cursor() as cur, open('/opt/airflow/data/housing_data.csv', 'w') as f:
      cur.copy_expert(
        sql=f"COPY housing_data TO STDOUT WITH CSV HEADER DELIMITER ','",
        file=f
      )

  print("housing_data exported to csv successfully")