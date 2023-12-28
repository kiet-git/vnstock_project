from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from data_crawling import *


def my_python_function(**kwargs):
    print("Executing my Python function!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 28),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'quarterly_crawl_dag',
    default_args=default_args,
    schedule_interval='0 16 1 */3 *', 
)

single_task = PythonOperator(
    task_id='single_task',
    python_callable=capture_all_data,
    op_args=[datetime.now().date(), None, 1],
    dag=dag,
)
