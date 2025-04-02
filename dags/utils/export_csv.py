from airflow.decorators import task, dag
import psycopg2

default_args = {
    'owner': 'airflow',
    'retries': 0,
}

@dag(
  dag_id='export_csv__never',
  default_args=default_args,
  schedule_interval=None,
  catchup=False,
  description='Utility DAG for exporting housing_data as csv')
def export_housing_data():

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
  
  export_data()

export_housing_data = export_housing_data()