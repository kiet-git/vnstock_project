from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="daily_crawl_dag",
    schedule='0 10 * * 1-5',
    start_date=days_ago(1),
    default_args=default_args,
    tags=["crawl"],
    description='DAG to craw data daily'
) as dag:
    from modules.data_crawling import capture_all_data
    
    single_task = PythonOperator(
        task_id='single_task',
        python_callable=capture_all_data,
        op_args=[datetime.now().date(), 10, 0],
        dag=dag,
    )
