from airflow.decorators import task, dag

@dag(schedule_interval=None)
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

    export_query = """
      COPY housing_data TO /opt/airflow/data/housing_data.csv DELIMITER ',' CSV HEADER;
    """

    cur.execute(export_query)
    conn.commit()

    cur.close()
    conn.close()
  
  export_data()
