from airflow.decorators import task, dag
import psycopg2
from tasks.utils.export_housing_data import export_data

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

  
  export_data()

export_housing_data = export_housing_data()